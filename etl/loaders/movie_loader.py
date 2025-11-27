"""
Рефакторинг MovieLoader с поддержкой нескольких стратегий загрузки.

Использование:
    # Discover (топ 10k)
    loader = MovieLoader(strategy="discover", target_count=10000)
    
    # Segmented (топ 50k)
    loader = MovieLoader(strategy="discover-segmented", target_count=50000, year_from=1990)
    
    # Export (legacy)
    loader = MovieLoader(strategy="export", target_count=1000, min_popularity=20)
"""

import asyncio
import os
from typing import List, Dict, Optional
from dotenv import load_dotenv

# Strategies
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from strategies import DiscoverStrategy, DiscoverSegmentedStrategy, ExportStrategy

# Базовый класс и утилиты
from base_loader import BaseLoader
from tmdb_client import AsyncTMDBClient

load_dotenv()


class MovieLoader(BaseLoader):
    """
    Универсальный загрузчик фильмов с поддержкой разных стратегий.
    """
    
    STRATEGIES = {
        "discover": DiscoverStrategy,
        "discover-segmented": DiscoverSegmentedStrategy,
        "export": ExportStrategy
    }
    
    def __init__(
        self,
        strategy: str = "discover",
        target_count: int = 10000,
        **strategy_kwargs
    ):
        """
        Args:
            strategy: Стратегия загрузки ID ("discover", "discover-segmented", "export")
            target_count: Количество фильмов
            **strategy_kwargs: Параметры для конкретной стратегии
        """
        super().__init__()
        
        self.strategy_name = strategy
        self.target_count = target_count
        self.strategy_kwargs = strategy_kwargs
        
        # TMDB клиенты
        self.bearer_token = os.getenv("TMDB_BEARER_TOKEN")
        if not self.bearer_token:
            raise ValueError("TMDB_BEARER_TOKEN not found in .env")
        
        self.async_client = AsyncTMDBClient()
        self.target_locales = os.getenv("TARGET_LOCALES", "en,ru").split(",")
        
        # Справочники (заполним в __enter__)
        self.genre_map = {}
        self.country_map = {}
    
    def __enter__(self):
        """Context manager: открываем БД и загружаем справочники"""
        super().__enter__()
        self._load_reference_data()
        return self
    
    def _load_reference_data(self):
        """Загрузка справочников из БД"""
        print("  Loading reference data...")
        
        self.cursor.execute("SELECT id, genre FROM content_service.genres")
        for row in self.cursor.fetchall():
            genre_id, genre = row
            self.genre_map[genre] = genre_id
        
        self.cursor.execute("SELECT id, iso_code FROM content_service.countries")
        for row in self.cursor.fetchall():
            country_id, iso_code = row
            self.country_map[iso_code] = country_id
        
        print(f"  ✅ Loaded {len(self.genre_map)} genres, {len(self.country_map)} countries\n")
    
    def _create_strategy(self):
        """Создание стратегии на основе параметров"""
        if self.strategy_name not in self.STRATEGIES:
            raise ValueError(f"Unknown strategy: {self.strategy_name}. Available: {list(self.STRATEGIES.keys())}")
        
        strategy_class = self.STRATEGIES[self.strategy_name]
        
        # Общие параметры
        common_kwargs = {
            "target_count": self.target_count
        }
        
        # Для discover/segmented нужен bearer_token
        if self.strategy_name in ["discover", "discover-segmented"]:
            common_kwargs["bearer_token"] = self.bearer_token
        
        # Объединяем с пользовательскими параметрами
        all_kwargs = {**common_kwargs, **self.strategy_kwargs}
        
        return strategy_class(**all_kwargs)
    
    def extract(self) -> List[Dict]:
        """
        Извлечение данных:
        1. Получить ID через выбранную стратегию
        2. Загрузить детали асинхронно
        """
        # Шаг 1: Получить ID
        print(f"\n{'='*60}")
        print(f"STRATEGY: {self.strategy_name}")
        print(f"{'='*60}")
        
        strategy = self._create_strategy()
        
        if self.strategy_name in ["discover", "discover-segmented"]:
            # Асинхронная стратегия
            movie_ids = asyncio.run(strategy.get_movie_ids())
        else:
            # Синхронная стратегия (export)
            movie_ids = strategy.get_movie_ids()
        
        if not movie_ids:
            print("⚠️  No movie IDs obtained from strategy")
            return []
        
        # Шаг 2: Загрузить детали асинхронно
        print(f"\n📥 Fetching details for {len(movie_ids)} movies...")
        print(f"⏱️  Estimated time: {len(movie_ids) / 45 / 60:.1f} minutes\n")
        
        movies_data = asyncio.run(
            self.async_client.fetch_movies_batch(
                movie_ids,
                progress_desc="Fetching movie details"
            )
        )
        
        print(f"\n✅ Fetched {len(movies_data)} movies successfully")
        return movies_data
    
    def transform(self, raw_data: List[Dict]) -> Dict[str, List]:
        """
        Трансформация TMDB данных в формат БД.
        
        Returns:
            Dict с ключами: content, movie_details, translations, genres, countries
        """
        print("\n⚙️  Transforming data...")
        
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
        
        print(f"✅ Transformed {len(raw_data)} movies")
        
        return {
            "content": content_data,
            "movie_details": movie_details_data,
            "translations": translations_data,
            "genres": genres_data,
            "countries": countries_data
        }
    
    def get_upsert_query(self) -> str:
        """Не используется (используем _load_table)"""
        pass
    
    def load(self, data: Dict[str, List]):
        """Загрузка всех таблиц"""
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
                for i in range(0, len(data["content"]), self.batch_size):
                    batch = data["content"][i:i + self.batch_size]
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
                for i in range(0, len(data["movie_details"]), self.batch_size):
                    batch = data["movie_details"][i:i + self.batch_size]
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
                for i in range(0, len(data["translations"]), self.batch_size):
                    batch = data["translations"][i:i + self.batch_size]
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
                for i in range(0, len(data["genres"]), self.batch_size):
                    batch = data["genres"][i:i + self.batch_size]
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
                for i in range(0, len(data["countries"]), self.batch_size):
                    batch = data["countries"][i:i + self.batch_size]
                    execute_batch(self.cursor, query, batch)
                    pbar.update(len(batch))
    
    def run(self):
        """Запуск ETL: extract → transform → load"""
        print(f"\n{'='*60}")
        print(f"Movie Loader")
        print(f"Strategy: {self.strategy_name}")
        print(f"Target: {self.target_count} movies")
        print(f"{'='*60}\n")
        
        with self:
            # Extract
            raw_data = self.extract()
            if not raw_data:
                print("⚠️  No data extracted")
                return
            
            # Transform
            transformed = self.transform(raw_data)
            
            # Load
            self.load(transformed)
        
        print(f"\n✅ Movie Loader completed successfully\n")


# =============================================================================
# EXAMPLE USAGE
# =============================================================================

if __name__ == "__main__":
    # Пример 1: Топ 10k через discover
    # loader = MovieLoader(
    #     strategy="discover",
    #     target_count=10000,
    #     sort_by="popularity.desc",
    #     min_vote_count=500
    # )
    # loader.run()
    
    # Пример 2: Топ 50k через segmentation
    loader = MovieLoader(
        strategy="discover-segmented",
        target_count=50000,
        year_from=1990,
        min_vote_count=100
    )
    loader.run()
    
    # Пример 3: Legacy через export
    # loader = MovieLoader(
    #     strategy="export",
    #     target_count=1000,
    #     min_popularity=20.0
    # )
    # loader.run()