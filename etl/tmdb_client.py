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
    
    def get_movie_ids(self) -> List[int]:
        """Получить список ID из daily export"""
        from loaders.id_export_loader import IDExportLoader
        
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
        actual_rate = len(results) / elapsed
        
        print(f"\n✅ Fetched {len(results)} movies in {elapsed:.1f}s")
        print(f"📊 Actual rate: {actual_rate:.1f} req/s")
        
        return results
    
    def run(self):
        """Запуск асинхронной загрузки"""
        print(f"\n{'='*60}")
        print(f"Async Movie Loader")
        print(f"Target: {self.target_count} movies (min popularity: {self.min_popularity})")
        print(f"{'='*60}\n")
        
        # Получаем ID
        movie_ids = self.get_movie_ids()
        
        # Загружаем асинхронно
        movies_data = asyncio.run(self.fetch_all_movies(movie_ids))
        
        # Трансформация и загрузка в БД
        print("\n⚙️  Transforming data...")
        transformed = self.transform(movies_data)
        
        print("\n📤 Loading to database...")
        self.load_to_db(transformed)
        
        print(f"\n✅ Completed successfully\n")
    
    def transform(self, raw_data: List[Dict]) -> Dict[str, List]:
        """Трансформация данных (как в MovieDetailsLoader)"""
        # TODO: реализовать трансформацию
        pass
    
    def load_to_db(self, data: Dict[str, List]):
        """Загрузка в БД (как в MovieDetailsLoader)"""
        # TODO: реализовать загрузку
        pass


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