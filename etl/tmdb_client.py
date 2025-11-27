"""
Асинхронный загрузчик с параллельными запросами.
Использует лимиты TMDB: 50 req/s и 20 одновременных соединений.
"""

import asyncio
import aiohttp
import time
import os
from typing import List, Dict, Optional
from tqdm.asyncio import tqdm_asyncio
from dotenv import load_dotenv

load_dotenv()


class AsyncTMDBClient:
    """
    Асинхронный TMDB клиент с параллельными запросами.
    
    Лимиты TMDB:
    - 50 requests/second
    - 20 одновременных соединений на IP
    
    Стратегия: используем 18 параллельных соединений (запас под лимит 20)
    и ~40-45 req/s (запас под лимит 50).
    """
    
    BASE_URL = "https://api.themoviedb.org/3"
    MAX_CONCURRENT = 18  # Одновременных соединений (из 20 доступных)
    REQUESTS_PER_SECOND = 45  # Безопасный предел (из 50 доступных)
    
    def __init__(self):
        self.bearer_token = os.getenv("TMDB_BEARER_TOKEN")
        if not self.bearer_token:
            raise ValueError("TMDB_BEARER_TOKEN not found in env")
        
        self.headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "accept": "application/json"
        }
        
        # Семафор для ограничения одновременных соединений
        self.semaphore = asyncio.Semaphore(self.MAX_CONCURRENT)
        
        # Rate limiter для req/s
        self.request_times = []
    
    async def _rate_limit(self):
        """Контроль частоты запросов (req/s)"""
        now = time.time()
        
        # Удаляем запросы старше 1 секунды
        self.request_times = [t for t in self.request_times if now - t < 1.0]
        
        # Если достигли лимита - ждём
        if len(self.request_times) >= self.REQUESTS_PER_SECOND:
            sleep_time = 1.0 - (now - self.request_times[0])
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            self.request_times = []
        
        self.request_times.append(now)
    
    async def _request(
        self, 
        session: aiohttp.ClientSession, 
        endpoint: str, 
        params: Optional[Dict] = None
    ) -> Optional[Dict]:
        """Базовый асинхронный запрос с rate limiting"""
        
        async with self.semaphore:  # Ограничение одновременных соединений
            await self._rate_limit()  # Rate limiting по req/s
            
            url = f"{self.BASE_URL}{endpoint}"
            
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:
                        retry_after = int(response.headers.get('Retry-After', 2))
                        print(f"⚠️  Rate limit hit, waiting {retry_after}s")
                        await asyncio.sleep(retry_after)
                        return await self._request(session, endpoint, params)
                    elif response.status == 404:
                        return None
                    else:
                        print(f"❌ HTTP {response.status}: {url}")
                        return None
            
            except Exception as e:
                print(f"❌ Request error: {e}")
                return None
    
    async def get_movie_full_data(
        self, 
        session: aiohttp.ClientSession, 
        movie_id: int
    ) -> Optional[Dict]:
        """Получить полные данные фильма (details + translations + credits)"""
        return await self._request(
            session,
            f"/movie/{movie_id}",
            params={
                "language": "en",
                "append_to_response": "translations,credits"
            }
        )
    
    async def fetch_movies_batch(
        self, 
        movie_ids: List[int],
        progress_desc: str = "Fetching movies"
    ) -> List[Dict]:
        """
        Загрузить батч фильмов параллельно.
        
        Использует 18 параллельных соединений и ~45 req/s.
        """
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            tasks = [
                self.get_movie_full_data(session, movie_id) 
                for movie_id in movie_ids
            ]
            
            # Выполняем с прогресс-баром
            results = await tqdm_asyncio.gather(*tasks, desc=progress_desc)
            
            # Фильтруем None (не найденные фильмы)
            return [r for r in results if r is not None]


"""
Асинхронный загрузчик фильмов с полной реализацией transform и load_to_db
Добавьте это в конец файла etl/tmdb_client.py (заменив существующий класс)
"""

class AsyncMovieDetailsLoader:
    """
    Асинхронный загрузчик фильмов.
    
    Производительность:
    - 18 параллельных соединений
    - ~45 req/s
    - ~2700 фильмов в минуту!
    """
    
    def __init__(self, target_count: int = 1000, min_popularity: float = 20):
        self.client = AsyncTMDBClient()
        self.target_count = target_count
        self.min_popularity = min_popularity
        self.target_locales = os.getenv("TARGET_LOCALES", "en,ru").split(",")
        
        # Для БД
        self.conn = None
        self.cursor = None
        self.genre_map = {}
        self.country_map = {}
    
    def __enter__(self):
        """Context manager для БД"""
        from db import get_connection
        self.conn = get_connection()
        self.cursor = self.conn.cursor()
        self._load_reference_data()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.conn.commit()
            print("✅ Transaction committed")
        else:
            self.conn.rollback()
            print(f"❌ Transaction rolled back: {exc_val}")
        
        self.cursor.close()
        self.conn.close()
    
    def _load_reference_data(self):
        """Загрузка справочников для маппинга"""
        print("  Loading reference data...")
        
        self.cursor.execute("SELECT id, genre FROM content_service.genres")
        for row in self.cursor.fetchall():
            genre_id, genre = row
            self.genre_map[genre] = genre_id
        
        self.cursor.execute("SELECT id, iso_code FROM content_service.countries")
        for row in self.cursor.fetchall():
            country_id, iso_code = row
            self.country_map[iso_code] = country_id
        
        print(f"  Loaded {len(self.genre_map)} genres, {len(self.country_map)} countries")
    
    def get_movie_ids(self) -> List[int]:
        """Получить список ID из daily export"""
        from loaders.id_export_loader import IDExportLoader
        
        print(f"\n📥 Fetching top {self.target_count} movie IDs...")
        
        id_loader = IDExportLoader(media_type="movie")
        return id_loader.get_filtered_ids(
            filters={
                "min_popularity": self.min_popularity,
                "exclude_adult": True,
                "exclude_video": True
            },
            limit=self.target_count
        )
    
    async def fetch_all_movies(self, movie_ids: List[int]) -> List[Dict]:
        """Загрузить все фильмы асинхронно"""
        print(f"\n📥 Fetching {len(movie_ids)} movies asynchronously...")
        print(f"⚡ Using {self.client.MAX_CONCURRENT} parallel connections")
        print(f"⚡ Rate limit: ~{self.client.REQUESTS_PER_SECOND} req/s")
        print(f"⏱️  Estimated time: {len(movie_ids) / (self.client.REQUESTS_PER_SECOND * 0.9) / 60:.1f} minutes\n")
        
        start = time.time()
        
        results = await self.client.fetch_movies_batch(
            movie_ids, 
            progress_desc="Fetching movies"
        )
        
        elapsed = time.time() - start
        actual_rate = len(results) / elapsed if elapsed > 0 else 0
        
        print(f"\n✅ Fetched {len(results)} movies in {elapsed:.1f}s")
        print(f"📊 Actual rate: {actual_rate:.1f} req/s")
        
        return results
    
    def transform(self, raw_data: List[Dict]) -> Dict[str, List]:
        """
        Трансформация данных из TMDB в формат БД.
        
        Returns:
            Dict с ключами: content, movie_details, translations, genres, countries
        """
        content_data = []
        movie_details_data = []
        translations_data = []
        genres_data = []
        countries_data = []
        
        for movie in raw_data:
            tmdb_id = movie["id"]
            
            # 1. content
            content_data.append((
                tmdb_id,
                movie.get("original_title", "Unknown"),
                "movie",
                movie.get("poster_path"),
                movie.get("release_date"),
                "published",
                None,  # age_rating
                movie.get("budget"),
                movie.get("revenue")
            ))
            
            # 2. movie_details
            runtime = movie.get("runtime")
            if runtime:
                movie_details_data.append((
                    tmdb_id,
                    runtime,
                    movie.get("release_date"),
                    None  # digital_release_date
                ))
            
            # 3. translations
            translations = movie.get("translations", {}).get("translations", [])
            for translation in translations:
                iso_639_1 = translation.get("iso_639_1")
                data = translation.get("data", {})
                title = data.get("title") or movie.get("original_title")
                overview = data.get("overview")
                
                if iso_639_1 in self.target_locales:
                    translations_data.append((
                        tmdb_id,
                        iso_639_1,
                        title,
                        overview,
                        None  # plot_summary
                    ))
            
            # 4. genres
            for idx, genre in enumerate(movie.get("genres", [])):
                genre_name = genre["name"].lower().replace(" ", "_")
                if genre_name in self.genre_map:
                    genres_data.append((
                        tmdb_id,
                        self.genre_map[genre_name],
                        idx
                    ))
            
            # 5. countries
            for country in movie.get("production_countries", []):
                iso_code = country["iso_3166_1"]
                if iso_code in self.country_map:
                    countries_data.append((
                        tmdb_id,
                        self.country_map[iso_code]
                    ))
        
        return {
            "content": content_data,
            "movie_details": movie_details_data,
            "translations": translations_data,
            "genres": genres_data,
            "countries": countries_data
        }
    
    def load_to_db(self, data: Dict[str, List]):
        """Загрузка в БД"""
        from psycopg2.extras import execute_batch
        from tqdm import tqdm
        
        # 1. content
        if data["content"]:
            print(f"\n📤 Loading content ({len(data['content'])} records)...")
            query = """
                INSERT INTO content_service.content 
                (id, original_title, content_type, poster_url, release_date, status, age_rating, budget, box_office)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    original_title = EXCLUDED.original_title,
                    poster_url = EXCLUDED.poster_url,
                    release_date = EXCLUDED.release_date,
                    budget = EXCLUDED.budget,
                    box_office = EXCLUDED.box_office,
                    updated_at = NOW()
            """
            with tqdm(total=len(data["content"]), desc="Loading content") as pbar:
                for i in range(0, len(data["content"]), 1000):
                    batch = data["content"][i:i + 1000]
                    execute_batch(self.cursor, query, batch)
                    pbar.update(len(batch))
        
        # 2. movie_details
        if data["movie_details"]:
            print(f"\n📤 Loading movie_details ({len(data['movie_details'])} records)...")
            query = """
                INSERT INTO content_service.movie_details 
                (content_id, duration_minutes, cinema_release_date, digital_release_date)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (content_id) DO UPDATE SET
                    duration_minutes = EXCLUDED.duration_minutes,
                    cinema_release_date = EXCLUDED.cinema_release_date,
                    updated_at = NOW()
            """
            with tqdm(total=len(data["movie_details"]), desc="Loading movie_details") as pbar:
                for i in range(0, len(data["movie_details"]), 1000):
                    batch = data["movie_details"][i:i + 1000]
                    execute_batch(self.cursor, query, batch)
                    pbar.update(len(batch))
        
        # 3. translations
        if data["translations"]:
            print(f"\n📤 Loading translations ({len(data['translations'])} records)...")
            query = """
                INSERT INTO content_service.content_translations 
                (content_id, locale, title, description, plot_summary)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (content_id, locale) DO UPDATE SET
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    updated_at = NOW()
            """
            with tqdm(total=len(data["translations"]), desc="Loading translations") as pbar:
                for i in range(0, len(data["translations"]), 1000):
                    batch = data["translations"][i:i + 1000]
                    execute_batch(self.cursor, query, batch)
                    pbar.update(len(batch))
        
        # 4. genres
        if data["genres"]:
            print(f"\n📤 Loading genres ({len(data['genres'])} records)...")
            query = """
                INSERT INTO content_service.content_genres (content_id, genre_id, display_order)
                VALUES (%s, %s, %s)
                ON CONFLICT (content_id, genre_id) DO UPDATE SET
                    display_order = EXCLUDED.display_order
            """
            with tqdm(total=len(data["genres"]), desc="Loading genres") as pbar:
                for i in range(0, len(data["genres"]), 1000):
                    batch = data["genres"][i:i + 1000]
                    execute_batch(self.cursor, query, batch)
                    pbar.update(len(batch))
        
        # 5. countries
        if data["countries"]:
            print(f"\n📤 Loading countries ({len(data['countries'])} records)...")
            query = """
                INSERT INTO content_service.content_countries (content_id, country_id)
                VALUES (%s, %s)
                ON CONFLICT (content_id, country_id) DO NOTHING
            """
            with tqdm(total=len(data["countries"]), desc="Loading countries") as pbar:
                for i in range(0, len(data["countries"]), 1000):
                    batch = data["countries"][i:i + 1000]
                    execute_batch(self.cursor, query, batch)
                    pbar.update(len(batch))
    
    def run(self):
        """Запуск асинхронной загрузки"""
        print(f"\n{'='*60}")
        print(f"Async Movie Loader")
        print(f"Target: {self.target_count} movies (min popularity: {self.min_popularity})")
        print(f"{'='*60}\n")
        
        with self:
            # 1. Получаем ID из export
            movie_ids = self.get_movie_ids()
            
            # 2. Загружаем асинхронно через API
            movies_data = asyncio.run(self.fetch_all_movies(movie_ids))
            
            # 3. Трансформация
            print("\n⚙️  Transforming data...")
            transformed = self.transform(movies_data)
            print(f"✅ Transformed {len(movies_data)} movies")
            
            # 4. Загрузка в БД
            self.load_to_db(transformed)
        
        print(f"\n✅ Completed successfully\n")

# Утилита для теста скорости
async def benchmark_api():
    """Тест максимальной скорости API"""
    client = AsyncTMDBClient()
    
    # Тестовые ID популярных фильмов
    test_ids = [550, 680, 155, 19404, 13, 24428, 11, 299534, 122, 101]
    
    print("🧪 Benchmarking TMDB API...")
    print(f"Testing with {len(test_ids)} movies\n")
    
    start = time.time()
    results = await client.fetch_movies_batch(test_ids, "Benchmark")
    elapsed = time.time() - start
    
    print(f"\n📊 Results:")
    print(f"  - Total time: {elapsed:.2f}s")
    print(f"  - Requests/second: {len(test_ids) / elapsed:.1f}")
    print(f"  - Success rate: {len(results)}/{len(test_ids)}")
    print(f"\n💡 Projected time for 10,000 movies: {10000 / (len(test_ids) / elapsed) / 60:.1f} minutes")


if __name__ == "__main__":
    # Запуск бенчмарка
    # asyncio.run(benchmark_api())
    
    # Или загрузка фильмов
    loader = AsyncMovieDetailsLoader(target_count=100, min_popularity=30)
    loader.run()