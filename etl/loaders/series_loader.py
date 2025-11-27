"""
Загрузчик сериалов с поддержкой сезонов и эпизодов (ИСПРАВЛЕНО).

АРХИТЕКТУРА:
1. content (основная запись сериала)
2. series_details (метаданные: total_seasons, total_episodes)
3. seasons (сезоны с их метаданными)
4. season_translations (переводы названий сезонов)
5. episodes (эпизоды) - ОПЦИОНАЛЬНО для MVP
6. episode_translations (переводы эпизодов) - ОПЦИОНАЛЬНО

ДЛЯ MVP: грузим только до уровня seasons, эпизоды добавим позже
"""

import asyncio
from typing import List, Dict, Tuple
from base_loader import BaseLoader
import os
import json


class SeriesLoader(BaseLoader):
    """
    Загрузчик сериалов с поддержкой strategies.
    
    Usage:
        # Топ 5000 сериалов БЕЗ эпизодов (быстро, для MVP)
        loader = SeriesLoader(
            strategy="discover",
            target_count=5000,
            load_episodes=False
        )
        
        # Топ 1000 сериалов С эпизодами (медленно)
        loader = SeriesLoader(
            strategy="discover",
            target_count=1000,
            load_episodes=True
        )
    """
    
    def __init__(
        self,
        strategy: str = "discover",
        target_count: int = 5000,
        load_episodes: bool = False,
        min_vote_count: int = 100,
        **strategy_kwargs
    ):
        super().__init__()
        
        self.strategy_name = strategy
        self.target_count = target_count
        self.load_episodes = load_episodes
        self.min_vote_count = min_vote_count
        self.strategy_kwargs = strategy_kwargs
        
        self.target_locales = os.getenv("TARGET_LOCALES", "en,ru").split(",")
        
        # Справочники (загрузим в _load_reference_data)
        self.genre_map = {}
        self.country_map = {}
        
        # Клиент и стратегию создаем в extract() внутри async контекста
        self.client = None
        self.strategy = None
    
    def _create_client_and_strategy(self):
        """
        Создать клиент и стратегию.
        ВАЖНО: вызывать только внутри async функции!
        """
        from tmdb_client import AsyncTMDBClient
        
        # Создаем НОВЫЙ клиент для каждого запуска
        self.client = AsyncTMDBClient()
        
        if self.strategy_name == "discover":
            from strategies.series_discover_strategy import SeriesDiscoverStrategy
            self.strategy = SeriesDiscoverStrategy(
                client=self.client,
                target_count=self.target_count,
                load_episodes=self.load_episodes,
                min_vote_count=self.min_vote_count,
                **self.strategy_kwargs
            )
        else:
            raise ValueError(f"Unknown strategy: {self.strategy_name}")
    
    def _load_reference_data(self):
        """Загрузка справочников"""
        print("  Loading reference data...")
        
        self.cursor.execute("SELECT id, genre FROM content_service.genres")
        self.genre_map = {row[1]: row[0] for row in self.cursor.fetchall()}
        
        self.cursor.execute("SELECT id, iso_code FROM content_service.countries")
        self.country_map = {row[1]: row[0] for row in self.cursor.fetchall()}
        
        print(f"  ✓ {len(self.genre_map)} genres, {len(self.country_map)} countries")
    
    def extract(self) -> List[Dict]:
        """
        Извлечение данных сериалов через стратегию.
        """
        self._load_reference_data()
        
        # Создаем клиент и стратегию внутри async контекста
        async def _async_extract():
            # Создаем клиент ВНУТРИ async функции
            self._create_client_and_strategy()
            
            # Оценка времени
            print(f"\n⏱️  Estimated time: {self.strategy.estimate_time()}")
            
            # Шаг 1: Получить ID сериалов
            series_ids = await self.strategy.get_series_ids()
            
            # Шаг 2: Загрузить полные данные
            series_data = await self.strategy.fetch_series_full_data(series_ids)
            
            return series_data
        
        # Запускаем async функцию
        return asyncio.run(_async_extract())
    
    def transform(self, raw_data: List[Dict]) -> Dict[str, List[Tuple]]:
        """
        Трансформация сериалов в формат БД.
        
        Returns:
            Dict с ключами:
            - content: базовые данные сериалов
            - series_details: метаданные сериалов
            - translations: переводы сериалов
            - genres: связь сериалы-жанры
            - countries: связь сериалы-страны
            - seasons: данные сезонов
            - season_translations: переводы сезонов
            - episodes: данные эпизодов (если load_episodes=True)
            - episode_translations: переводы эпизодов (если load_episodes=True)
        """
        content_data = []
        series_details_data = []
        translations_data = []
        genres_data = []
        countries_data = []
        seasons_data = []
        season_translations_data = []
        episodes_data = []
        episode_translations_data = []
        
        for series in raw_data:
            tmdb_id = series["id"]
            
            # 1. content (основная запись)
            content_data.append((
                tmdb_id,
                series.get("original_name", series.get("name", "Unknown")),
                "series",  # content_type
                series.get("poster_path"),
                series.get("first_air_date"),  # release_date
                "published",
                None,  # age_rating
                None,  # budget (нет для сериалов)
                None   # box_office (нет для сериалов)
            ))
            
            # 2. series_details
            seasons = series.get("seasons", [])
            # Считаем реальные сезоны (пропускаем season 0 = specials)
            real_seasons = [s for s in seasons if s.get("season_number", 0) > 0]
            
            total_seasons = len(real_seasons)
            total_episodes = series.get("number_of_episodes", 0)
            avg_episode_duration = None
            
            # Пытаемся вычислить среднюю длительность
            episode_runtimes = series.get("episode_run_time", [])
            if episode_runtimes:
                avg_episode_duration = sum(episode_runtimes) // len(episode_runtimes)
            
            series_details_data.append((
                tmdb_id,
                total_seasons,
                total_episodes,
                avg_episode_duration,
                series.get("last_air_date"),  # end_date
                self._get_series_status(series)
            ))
            
            # 3. content_translations
            translations = series.get("translations", {}).get("translations", [])
            for translation in translations:
                iso_639_1 = translation.get("iso_639_1")
                data = translation.get("data", {})
                title = data.get("name") or series.get("original_name")
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
            for idx, genre in enumerate(series.get("genres", [])):
                genre_name = genre["name"].lower().replace(" ", "_")
                if genre_name in self.genre_map:
                    genres_data.append((
                        tmdb_id,
                        self.genre_map[genre_name],
                        idx
                    ))
            
            # 5. countries
            for country in series.get("production_countries", []):
                iso_code = country["iso_3166_1"]
                if iso_code in self.country_map:
                    countries_data.append((
                        tmdb_id,
                        self.country_map[iso_code]
                    ))
            
            # 6. seasons
            for season in real_seasons:
                season_number = season["season_number"]
                
                seasons_data.append((
                    tmdb_id,
                    season_number,
                    season.get("poster_path"),
                    season.get("air_date"),
                    season.get("episode_count", 0)
                ))
                
                # 7. season_translations (базовые - из API)
                for locale in self.target_locales:
                    season_name = season.get("name", f"Season {season_number}")
                    season_overview = season.get("overview")
                    
                    season_translations_data.append((
                        tmdb_id,
                        season_number,
                        locale,
                        season_name,
                        season_overview
                    ))
                
                # 8. episodes (если загружены)
                if self.load_episodes:
                    for episode in season.get("episodes", []):
                        episode_number = episode["episode_number"]
                        
                        episodes_data.append((
                            tmdb_id,
                            season_number,
                            episode_number,
                            episode.get("runtime"),
                            episode.get("air_date")
                        ))
                        
                        # 9. episode_translations
                        for locale in self.target_locales:
                            episode_title = episode.get("name", f"Episode {episode_number}")
                            episode_overview = episode.get("overview")
                            
                            episode_translations_data.append((
                                tmdb_id,
                                season_number,
                                episode_number,
                                locale,
                                episode_title,
                                episode_overview,
                                None  # plot_summary
                            ))
        
        return {
            "content": content_data,
            "series_details": series_details_data,
            "translations": translations_data,
            "genres": genres_data,
            "countries": countries_data,
            "seasons": seasons_data,
            "season_translations": season_translations_data,
            "episodes": episodes_data,
            "episode_translations": episode_translations_data
        }
    
    def _get_series_status(self, series: Dict) -> str:
        """Определить статус сериала"""
        status = series.get("status", "").lower()
        
        if "ended" in status or "canceled" in status:
            return "finished"
        elif "returning" in status or "planned" in status:
            return "ongoing"
        else:
            return "ongoing"  # default
    
    def get_upsert_query(self) -> str:
        # Не используется, т.к. transform возвращает словарь
        pass
    
    def run(self):
        """Запуск загрузки сериалов"""
        print(f"\n{'='*60}")
        print(f"Series Loader")
        print(f"Strategy: {self.strategy_name}")
        print(f"Target: {self.target_count} series")
        print(f"Min vote count: {self.min_vote_count}")
        print(f"Load episodes: {self.load_episodes}")
        print(f"{'='*60}\n")
        
        with self:
            # Extract
            raw_data = self.extract()
            if not raw_data:
                print("⚠️  No data extracted")
                return
            
            # Transform
            print("\n⚙️  Transforming data...")
            transformed = self.transform(raw_data)
            print(f"✅ Transformed {len(raw_data)} series")
            
            # Load
            self._load_all_tables(transformed)
        
        print(f"\n✅ Series Loader completed successfully\n")
    
    def _load_all_tables(self, data: Dict[str, List]):
        """Загрузка всех таблиц в правильном порядке"""
        from psycopg2.extras import execute_batch
        from tqdm import tqdm
        
        # 1. content
        if data["content"]:
            print(f"\n📤 Loading content ({len(data['content'])} records)...")
            query = """
                INSERT INTO content_service.content 
                (id, original_title, content_type, poster_url, release_date, 
                 status, age_rating, budget, box_office)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    original_title = EXCLUDED.original_title,
                    poster_url = EXCLUDED.poster_url,
                    release_date = EXCLUDED.release_date,
                    updated_at = NOW()
            """
            self._batch_insert(query, data["content"], "content")
        
        # 2. series_details
        if data["series_details"]:
            print(f"\n📤 Loading series_details ({len(data['series_details'])} records)...")
            query = """
                INSERT INTO content_service.series_details 
                (content_id, total_seasons, total_episodes, average_episode_duration,
                 end_date, series_status)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (content_id) DO UPDATE SET
                    total_seasons = EXCLUDED.total_seasons,
                    total_episodes = EXCLUDED.total_episodes,
                    average_episode_duration = EXCLUDED.average_episode_duration,
                    end_date = EXCLUDED.end_date,
                    series_status = EXCLUDED.series_status,
                    updated_at = NOW()
            """
            self._batch_insert(query, data["series_details"], "series_details")
        
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
            self._batch_insert(query, data["translations"], "translations")
        
        # 4. genres
        if data["genres"]:
            print(f"\n📤 Loading genres ({len(data['genres'])} records)...")
            query = """
                INSERT INTO content_service.content_genres (content_id, genre_id, display_order)
                VALUES (%s, %s, %s)
                ON CONFLICT (content_id, genre_id) DO UPDATE SET
                    display_order = EXCLUDED.display_order
            """
            self._batch_insert(query, data["genres"], "genres")
        
        # 5. countries
        if data["countries"]:
            print(f"\n📤 Loading countries ({len(data['countries'])} records)...")
            query = """
                INSERT INTO content_service.content_countries (content_id, country_id)
                VALUES (%s, %s)
                ON CONFLICT (content_id, country_id) DO NOTHING
            """
            self._batch_insert(query, data["countries"], "countries")
        
        # 6. seasons
        if data["seasons"]:
            print(f"\n📤 Loading seasons ({len(data['seasons'])} records)...")
            query = """
                INSERT INTO content_service.seasons 
                (content_id, season_number, poster_url, release_date, episodes_count)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (content_id, season_number) DO UPDATE SET
                    poster_url = EXCLUDED.poster_url,
                    release_date = EXCLUDED.release_date,
                    episodes_count = EXCLUDED.episodes_count,
                    updated_at = NOW()
                RETURNING id, content_id, season_number
            """
            # Нужен особый подход - сохраняем season_id для translations
            season_id_map = {}  # (content_id, season_number) -> season_id
            
            with tqdm(total=len(data["seasons"]), desc="Loading seasons") as pbar:
                for i in range(0, len(data["seasons"]), self.batch_size):
                    batch = data["seasons"][i:i + self.batch_size]
                    
                    for row in batch:
                        self.cursor.execute(query, row)
                        result = self.cursor.fetchone()
                        if result:
                            season_id, content_id, season_number = result
                            season_id_map[(content_id, season_number)] = season_id
                    
                    pbar.update(len(batch))
            
            print(f"  ✓ Loaded {len(data['seasons'])} seasons")
            
            # 7. season_translations
            if data["season_translations"]:
                print(f"\n📤 Loading season_translations ({len(data['season_translations'])} records)...")
                
                season_trans_with_ids = []
                for row in data["season_translations"]:
                    content_id, season_number, locale, title, description = row
                    season_id = season_id_map.get((content_id, season_number))
                    if season_id:
                        season_trans_with_ids.append((
                            season_id, locale, title, description
                        ))
                
                query = """
                    INSERT INTO content_service.season_translations 
                    (season_id, locale, title, description)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (season_id, locale) DO UPDATE SET
                        title = EXCLUDED.title,
                        description = EXCLUDED.description,
                        updated_at = NOW()
                """
                self._batch_insert(query, season_trans_with_ids, "season_translations")
        
        # 8. episodes (если есть)
        if data["episodes"]:
            print(f"\n📤 Loading episodes ({len(data['episodes'])} records)...")
            
            # Получаем все season_id
            self.cursor.execute("""
                SELECT id, content_id, season_number 
                FROM content_service.seasons
            """)
            season_id_map = {
                (row[1], row[2]): row[0] 
                for row in self.cursor.fetchall()
            }
            
            # Преобразуем в (season_id, episode_number, ...)
            episodes_with_season_ids = []
            for row in data["episodes"]:
                content_id, season_number, episode_number, runtime, air_date = row
                season_id = season_id_map.get((content_id, season_number))
                if season_id:
                    episodes_with_season_ids.append((
                        season_id, episode_number, runtime, air_date
                    ))
            
            query = """
                INSERT INTO content_service.episodes 
                (season_id, episode_number, duration_minutes, air_date)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (season_id, episode_number) DO UPDATE SET
                    duration_minutes = EXCLUDED.duration_minutes,
                    air_date = EXCLUDED.air_date,
                    updated_at = NOW()
                RETURNING id, season_id, episode_number
            """
            
            episode_id_map = {}
            
            with tqdm(total=len(episodes_with_season_ids), desc="Loading episodes") as pbar:
                for i in range(0, len(episodes_with_season_ids), self.batch_size):
                    batch = episodes_with_season_ids[i:i + self.batch_size]
                    
                    for row in batch:
                        self.cursor.execute(query, row)
                        result = self.cursor.fetchone()
                        if result:
                            episode_id, season_id, episode_number = result
                            episode_id_map[(season_id, episode_number)] = episode_id
                    
                    pbar.update(len(batch))
            
            print(f"  ✓ Loaded {len(episodes_with_season_ids)} episodes")
            
            # 9. episode_translations
            if data["episode_translations"]:
                print(f"\n📤 Loading episode_translations ({len(data['episode_translations'])} records)...")
                
                episode_trans_with_ids = []
                for row in data["episode_translations"]:
                    content_id, season_num, ep_num, locale, title, desc, plot = row
                    season_id = season_id_map.get((content_id, season_num))
                    if season_id:
                        episode_id = episode_id_map.get((season_id, ep_num))
                        if episode_id:
                            episode_trans_with_ids.append((
                                episode_id, locale, title, desc, plot
                            ))
                
                query = """
                    INSERT INTO content_service.episode_translations 
                    (episode_id, locale, title, description, plot_summary)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (episode_id, locale) DO UPDATE SET
                        title = EXCLUDED.title,
                        description = EXCLUDED.description,
                        updated_at = NOW()
                """
                self._batch_insert(query, episode_trans_with_ids, "episode_translations")
    
    def _batch_insert(self, query: str, data: List, name: str):
        """Вспомогательный метод для batch insert"""
        from psycopg2.extras import execute_batch
        from tqdm import tqdm
        
        with tqdm(total=len(data), desc=f"Loading {name}") as pbar:
            for i in range(0, len(data), self.batch_size):
                batch = data[i:i + self.batch_size]
                execute_batch(self.cursor, query, batch, page_size=self.batch_size)
                pbar.update(len(batch))
        
        print(f"  ✓ Loaded {len(data)} {name}")


if __name__ == "__main__":
    # Тест: топ-100 сериалов БЕЗ эпизодов (быстро)
    loader = SeriesLoader(
        strategy="discover",
        target_count=100,
        load_episodes=False,
        min_vote_count=100
    )
    loader.run()