import requests
import gzip
import json
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from base_loader import BaseLoader
from tmdb_client import TMDBClient

class IDExportLoader:
    """
    Загрузчик ежедневных экспортов ID из TMDB.
    
    Формат файлов:
    - NDJSON (Newline Delimited JSON) - каждая строка это JSON объект
    - Gzipped
    - Структура: {"adult":false,"id":123,"original_title":"..","popularity":45.2,"video":false}
    """
    
    BASE_URL = "https://files.tmdb.org/p/exports"
    
    def __init__(self, media_type: str = "movie"):
        """
        Args:
            media_type: "movie", "tv_series", "person", "collection" и т.д.
        """
        self.media_type = media_type
        self.data_dir = os.getenv("DATA_DIR", "./data")
        os.makedirs(self.data_dir, exist_ok=True)
    
    def _get_latest_filename(self, days_back: int = 0) -> str:
        """
        Генерирует имя файла для заданной даты.
        TMDB публикует файлы в формате: movie_ids_MM_DD_YYYY.json.gz
        """
        date = datetime.now() - timedelta(days=days_back)
        date_str = date.strftime("%m_%d_%Y")
        return f"{self.media_type}_ids_{date_str}.json.gz"
    
    def download_export(self, max_retries: int = 7) -> Optional[str]:
        """
        Скачивает последний доступный export файл.
        TMDB хранит файлы только за последние 3 месяца.
        
        Returns:
            Путь к скачанному файлу или None
        """
        # Пробуем последние 7 дней (файлы публикуются ежедневно ~8 AM UTC)
        for days_back in range(max_retries):
            filename = self._get_latest_filename(days_back)
            url = f"{self.BASE_URL}/{filename}"
            local_path = os.path.join(self.data_dir, filename)
            
            # Проверяем кэш
            if os.path.exists(local_path):
                print(f"✓ Using cached file: {filename}")
                return local_path
            
            print(f"Trying to download: {url}")
            
            try:
                response = requests.get(url, stream=True, timeout=30)
                
                if response.status_code == 200:
                    # Скачиваем с прогресс-баром
                    file_size = int(response.headers.get('content-length', 0))
                    
                    print(f"Downloading {filename} ({file_size / 1024 / 1024:.2f} MB)...")
                    
                    with open(local_path, 'wb') as f:
                        downloaded = 0
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                            downloaded += len(chunk)
                            
                            # Простой прогресс
                            if file_size > 0:
                                percent = (downloaded / file_size) * 100
                                print(f"\r  Progress: {percent:.1f}%", end='', flush=True)
                    
                    print(f"\n✓ Downloaded: {filename}")
                    return local_path
                
                elif response.status_code == 403:
                    print(f"  Access denied (file might not exist yet)")
                else:
                    print(f"  HTTP {response.status_code}")
            
            except requests.RequestException as e:
                print(f"  Download failed: {e}")
        
        print(f"❌ Could not download export file after {max_retries} attempts")
        return None
    
    def parse_export(self, filepath: str, filters: Optional[Dict] = None) -> List[Dict]:
        """
        Парсит NDJSON export файл с опциональными фильтрами.
        
        Args:
            filepath: Путь к .json.gz файлу
            filters: Фильтры для отбора ID, например:
                {
                    "min_popularity": 10,
                    "exclude_adult": True,
                    "exclude_video": True  # прямой релиз на видео
                }
        
        Returns:
            Список отфильтрованных записей
        """
        filters = filters or {}
        min_popularity = filters.get("min_popularity", 0)
        exclude_adult = filters.get("exclude_adult", True)
        exclude_video = filters.get("exclude_video", True)
        
        results = []
        
        print(f"Parsing {os.path.basename(filepath)}...")
        
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    item = json.loads(line.strip())
                    
                    # Применяем фильтры
                    if exclude_adult and item.get("adult", False):
                        continue
                    
                    if exclude_video and item.get("video", False):
                        continue
                    
                    popularity = item.get("popularity", 0)
                    if popularity < min_popularity:
                        continue
                    
                    results.append(item)
                
                except json.JSONDecodeError as e:
                    print(f"  Warning: Invalid JSON on line {line_num}: {e}")
                    continue
        
        print(f"✓ Parsed {len(results)} valid items (from ~{line_num} total)")
        return results
    
    def get_filtered_ids(self, filters: Optional[Dict] = None, limit: Optional[int] = None) -> List[int]:
        """
        Полный цикл: скачать -> распарсить -> вернуть ID.
        
        Args:
            filters: Фильтры для отбора
            limit: Максимальное количество ID (берем top N по popularity)
        
        Returns:
            Список TMDB ID
        """
        # 1. Скачиваем export
        filepath = self.download_export()
        if not filepath:
            raise RuntimeError("Failed to download ID export")
        
        # 2. Парсим с фильтрами
        items = self.parse_export(filepath, filters)
        
        # 3. Сортируем по popularity (descending)
        items.sort(key=lambda x: x.get("popularity", 0), reverse=True)
        
        # 4. Берем top N
        if limit:
            items = items[:limit]
        
        # 5. Извлекаем ID
        ids = [item["id"] for item in items]
        
        print(f"✓ Selected {len(ids)} IDs")
        return ids


class MovieDetailsLoader(BaseLoader):
    """
    Загрузчик деталей фильмов на основе списка ID.
    Использует IDExportLoader для получения ID, затем качает детали через API.
    """
    
    def __init__(self, target_count: int = 1000, min_popularity: float = 20):
        super().__init__()
        self.client = TMDBClient()
        self.id_loader = IDExportLoader(media_type="movie")
        self.target_count = target_count
        self.min_popularity = min_popularity
        self.target_locales = os.getenv("TARGET_LOCALES", "en,ru").split(",")
        
        # Маппинги справочников
        self.genre_map = {}
        self.country_map = {}
    
    def _load_reference_data(self):
        """Загружаем справочники для маппинга"""
        print("  Loading reference data...")
        
        self.cursor.execute("SELECT id, genre FROM content_service.genres")
        self.genre_map = {row[1]: row[0] for row in self.cursor.fetchall()}
        
        self.cursor.execute("SELECT id, iso_code FROM content_service.countries")
        self.country_map = {row[1]: row[0] for row in self.cursor.fetchall()}
        
        print(f"  Loaded {len(self.genre_map)} genres, {len(self.country_map)} countries")
    
    def extract(self) -> List[Dict]:
        """
        1. Получаем список ID через daily export (быстро)
        2. Загружаем детали для каждого ID через API (медленно)
        """
        self._load_reference_data()
        
        # Шаг 1: Получить отфильтрованные ID
        print(f"\n📥 Fetching top {self.target_count} movie IDs (popularity >= {self.min_popularity})...")
        
        movie_ids = self.id_loader.get_filtered_ids(
            filters={
                "min_popularity": self.min_popularity,
                "exclude_adult": True,
                "exclude_video": True
            },
            limit=self.target_count
        )
        
        # Шаг 2: Загрузить детали через API
        print(f"\n📥 Fetching details for {len(movie_ids)} movies...")
        print(f"⚠️  This will take approximately {len(movie_ids) * 0.26 / 60:.1f} minutes")
        
        movies_data = []
        failed_ids = []
        
        from tqdm import tqdm
        
        for movie_id in tqdm(movie_ids, desc="Fetching movie details"):
            # Получаем детали
            details = self.client.get_movie_details(movie_id, language="en")
            if not details:
                failed_ids.append(movie_id)
                continue
            
            # Получаем переводы
            translations = self.client.get_movie_translations(movie_id)
            
            movies_data.append({
                "details": details,
                "translations": translations
            })
        
        if failed_ids:
            print(f"\n⚠️  Failed to fetch {len(failed_ids)} movies: {failed_ids[:10]}...")
        
        print(f"\n✓ Successfully fetched {len(movies_data)} movies")
        return movies_data
    
    def transform(self, raw_data: List[Dict]) -> Dict[str, List]:
        """
        Трансформация в формат для БД (как в MovieLoader).
        Возвращаем словарь с данными для всех связанных таблиц.
        """
        content_data = []
        movie_details_data = []
        translations_data = []
        genres_data = []
        countries_data = []
        
        for movie in raw_data:
            details = movie["details"]
            tmdb_id = details["id"]
            
            # 1. content
            content_data.append((
                tmdb_id,
                details.get("original_title", "Unknown"),
                "movie",
                details.get("poster_path"),
                details.get("release_date"),
                "published",
                None,
                details.get("budget"),
                details.get("revenue")
            ))
            
            # 2. movie_details
            runtime = details.get("runtime")
            if runtime:
                movie_details_data.append((
                    tmdb_id,
                    runtime,
                    details.get("release_date"),
                    None
                ))
            
            # 3. translations
            for translation in movie["translations"]:
                iso_639_1 = translation.get("iso_639_1")
                data = translation.get("data", {})
                title = data.get("title") or details.get("original_title")
                overview = data.get("overview")
                
                if iso_639_1 in self.target_locales:
                    translations_data.append((
                        tmdb_id,
                        iso_639_1,
                        title,
                        overview,
                        None
                    ))
            
            # 4. genres
            for idx, genre in enumerate(details.get("genres", [])):
                genre_name = genre["name"]
                if genre_name in self.genre_map:
                    genres_data.append((
                        tmdb_id,
                        self.genre_map[genre_name],
                        idx
                    ))
            
            # 5. countries
            for country in details.get("production_countries", []):
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
    
    def get_upsert_query(self) -> str:
        pass  # Не используется
    
    def run(self):
        """Запуск загрузки с множественными таблицами"""
        print(f"\n{'='*60}")
        print(f"Starting {self.__class__.__name__}")
        print(f"Target: {self.target_count} movies (min popularity: {self.min_popularity})")
        print(f"{'='*60}\n")
        
        with self:
            raw_data = self.extract()
            if not raw_data:
                print("⚠️  No data extracted")
                return
            
            print("\n⚙️  Transforming data...")
            transformed = self.transform(raw_data)
            
            # Загружаем таблицы
            self._load_table("content", transformed["content"], """
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
            """)
            
            self._load_table("movie_details", transformed["movie_details"], """
                INSERT INTO content_service.movie_details 
                (content_id, duration_minutes, cinema_release_date, digital_release_date)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (content_id) DO UPDATE SET
                    duration_minutes = EXCLUDED.duration_minutes,
                    cinema_release_date = EXCLUDED.cinema_release_date,
                    updated_at = NOW()
            """)
            
            self._load_table("translations", transformed["translations"], """
                INSERT INTO content_service.content_translations 
                (content_id, locale, title, description, plot_summary)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (content_id, locale) DO UPDATE SET
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    updated_at = NOW()
            """)
            
            self._load_table("genres", transformed["genres"], """
                INSERT INTO content_service.content_genres (content_id, genre_id, display_order)
                VALUES (%s, %s, %s)
                ON CONFLICT (content_id, genre_id) DO UPDATE SET
                    display_order = EXCLUDED.display_order
            """)
            
            self._load_table("countries", transformed["countries"], """
                INSERT INTO content_service.content_countries (content_id, country_id)
                VALUES (%s, %s)
                ON CONFLICT (content_id, country_id) DO NOTHING
            """)
        
        print(f"\n✅ {self.__class__.__name__} completed successfully\n")
    
    def _load_table(self, name: str, data: List, query: str):
        """Вспомогательный метод для загрузки одной таблицы"""
        if not data:
            print(f"  No {name} to load")
            return
        
        from psycopg2.extras import execute_batch
        from tqdm import tqdm
        
        print(f"\n📤 Loading {name}...")
        with tqdm(total=len(data), desc=f"Loading {name}") as pbar:
            for i in range(0, len(data), self.batch_size):
                batch = data[i:i + self.batch_size]
                execute_batch(self.cursor, query, batch, page_size=self.batch_size)
                pbar.update(len(batch))
        
        print(f"  ✓ Loaded {len(data)} {name}")


if __name__ == "__main__":
    # Тест: загрузить топ-100 популярных фильмов
    loader = MovieDetailsLoader(target_count=100, min_popularity=20)
    loader.run()