"""
Стратегия загрузки сериалов с ПРАВИЛЬНОЙ загрузкой переводов эпизодов

КРИТИЧЕСКИЕ ИСПРАВЛЕНИЯ:
1. Переводы эпизодов грузятся через /tv/{id}/season/{s}/episode/{e}/translations
2. Используется батчинг для избежания rate limit
3. Прогресс-бары показывают реальное время
"""

import asyncio
import aiohttp
from typing import List, Dict, Optional
from tqdm.asyncio import tqdm_asyncio


class SeriesDiscoverStrategy:
    """
    Стратегия загрузки сериалов с ПРАВИЛЬНОЙ обработкой эпизодов и переводов
    """
    
    def __init__(
        self,
        client,
        target_count: int = 5000,
        sort_by: str = "popularity.desc",
        min_vote_count: int = 100,
        load_episodes: bool = False,
        target_locales: List[str] = None
    ):
        """
        Args:
            client: AsyncTMDBClient instance
            target_count: Количество сериалов
            sort_by: Сортировка
            min_vote_count: Минимум голосов
            load_episodes: Загружать ли эпизоды С ПЕРЕВОДАМИ
            target_locales: Список локалей для переводов (из .env)
        """
        self.client = client
        self.target_count = target_count
        self.sort_by = sort_by
        self.min_vote_count = min_vote_count
        self.load_episodes = load_episodes
        self.target_locales = target_locales or ["en", "ru"]
        
        self.max_pages = 500
        self.results_per_page = 20
    
    async def get_series_ids(self) -> List[int]:
        """Получить список ID сериалов через /discover/tv"""
        print(f"📥 Discovering series (target: {self.target_count})...")
        print(f"   Sort: {self.sort_by}")
        print(f"   Min votes: {self.min_vote_count}\n")
        
        series_ids = []
        pages_needed = min(
            (self.target_count + self.results_per_page - 1) // self.results_per_page,
            self.max_pages
        )
        
        async with aiohttp.ClientSession(headers=self.client.headers) as session:
            tasks = []
            
            for page in range(1, pages_needed + 1):
                tasks.append(self._fetch_discover_page(session, page))
                
                if len(tasks) >= 50 or page == pages_needed:
                    results = await asyncio.gather(*tasks)
                    
                    for page_data in results:
                        if page_data:
                            series_ids.extend([s["id"] for s in page_data.get("results", [])])
                    
                    tasks = []
                    print(f"  Progress: {page}/{pages_needed} pages, {len(series_ids)} series found")
                    
                    if len(series_ids) >= self.target_count:
                        break
        
        series_ids = series_ids[:self.target_count]
        print(f"✅ Discovered {len(series_ids)} series IDs\n")
        return series_ids
    
    async def _fetch_discover_page(
        self, 
        session: aiohttp.ClientSession, 
        page: int
    ) -> Optional[Dict]:
        """Получить одну страницу discover results"""
        params = {
            "page": page,
            "sort_by": self.sort_by,
            "vote_count.gte": self.min_vote_count,
            "include_adult": "false"
        }
        
        return await self.client._request(session, "/discover/tv", params)
    
    async def fetch_series_full_data(self, series_ids: List[int]) -> List[Dict]:
        """
        Загрузить полные данные сериалов с эпизодами и переводами.
        
        КРИТИЧНО: Если load_episodes=True, грузим ВСЕ переводы для КАЖДОГО эпизода
        """
        print(f"📥 Fetching full data for {len(series_ids)} series...")
        
        if self.load_episodes:
            print("⚠️  WARNING: load_episodes=True will be VERY SLOW")
            print(f"   This will load TRANSLATIONS for EVERY episode")
            print(f"   Estimated time: {self._estimate_episodes_time(len(series_ids))}\n")
        
        async with aiohttp.ClientSession(headers=self.client.headers) as session:
            # Шаг 1: Загрузить базовые данные сериалов
            tasks = [
                self._fetch_series_with_seasons(session, series_id)
                for series_id in series_ids
            ]
            
            results = await tqdm_asyncio.gather(*tasks, desc="Fetching series")
            series_data = [r for r in results if r is not None]
            
            print(f"✅ Fetched {len(series_data)} series (with seasons)")
            
            # Шаг 2: Если нужны эпизоды - загружаем их с переводами
            if self.load_episodes:
                await self._fetch_all_episodes_with_translations(session, series_data)
            
            return series_data
    
    async def _fetch_series_with_seasons(
        self, 
        session: aiohttp.ClientSession, 
        series_id: int
    ) -> Optional[Dict]:
        """Получить сериал + его сезоны (БЕЗ эпизодов)"""
        data = await self.client._request(
            session,
            f"/tv/{series_id}",
            params={
                "language": "en",
                "append_to_response": "translations"
            }
        )
        
        if not data:
            return None
        
        # Инициализируем пустые списки эпизодов
        for season in data.get("seasons", []):
            season["episodes"] = []
        
        return data
    
    async def _fetch_all_episodes_with_translations(
        self, 
        session: aiohttp.ClientSession, 
        series_data: List[Dict]
    ):
        """
        КРИТИЧЕСКАЯ ФУНКЦИЯ: Загрузить все эпизоды СО ВСЕМИ переводами
        
        Алгоритм:
        1. Собрать все (series_id, season_number) пары
        2. Загрузить базовые данные эпизодов батчами
        3. Для КАЖДОГО эпизода загрузить ВСЕ переводы
        
        ВАЖНО: Это самая медленная часть всего ETL
        """
        print(f"\n📥 Fetching episodes WITH translations...")
        
        # Шаг 1: Собрать все сезоны для загрузки
        season_tasks = []
        for series in series_data:
            series_id = series["id"]
            for season in series.get("seasons", []):
                season_number = season["season_number"]
                if season_number > 0:  # Пропускаем Specials
                    season_tasks.append((series_id, season_number, season))
        
        print(f"  Total seasons: {len(season_tasks)}")
        
        # Шаг 2: Загрузить базовые данные эпизодов батчами
        episodes_map = {}  # {(series_id, season_num): [episodes]}
        
        batch_size = 100
        for i in range(0, len(season_tasks), batch_size):
            batch = season_tasks[i:i + batch_size]
            
            tasks = [
                self._fetch_season_episodes(session, series_id, season_num)
                for series_id, season_num, _ in batch
            ]
            
            results = await tqdm_asyncio.gather(
                *tasks, 
                desc=f"Fetching episodes batch {i//batch_size + 1}/{(len(season_tasks) + batch_size - 1)//batch_size}"
            )
            
            # Сохраняем эпизоды
            for (series_id, season_num, season_obj), episodes in zip(batch, results):
                if episodes:
                    episodes_map[(series_id, season_num)] = episodes
                    season_obj["episodes"] = episodes
        
        total_episodes = sum(len(eps) for eps in episodes_map.values())
        print(f"  ✅ Loaded {total_episodes} episodes (base data)")
        
        # Шаг 3: КРИТИЧНО - загрузить переводы для КАЖДОГО эпизода
        print(f"\n📥 Fetching translations for {total_episodes} episodes...")
        print(f"  This will take approximately {total_episodes / 45 / 60:.1f} minutes")
        
        # Собираем все (series_id, season_num, episode_num, episode_obj)
        episode_translation_tasks = []
        for (series_id, season_num), episodes in episodes_map.items():
            for episode in episodes:
                episode_num = episode["episode_number"]
                episode_translation_tasks.append((series_id, season_num, episode_num, episode))
        
        # Грузим переводы батчами
        batch_size = 200
        for i in range(0, len(episode_translation_tasks), batch_size):
            batch = episode_translation_tasks[i:i + batch_size]
            
            tasks = [
                self._fetch_episode_translations(session, series_id, season_num, episode_num)
                for series_id, season_num, episode_num, _ in batch
            ]
            
            results = await tqdm_asyncio.gather(
                *tasks,
                desc=f"Fetching translations batch {i//batch_size + 1}/{(len(episode_translation_tasks) + batch_size - 1)//batch_size}"
            )
            
            # Записываем переводы обратно в episode объекты
            for (_, _, _, episode_obj), translations in zip(batch, results):
                episode_obj["translations"] = translations or []
        
        print(f"  ✅ Loaded translations for {len(episode_translation_tasks)} episodes")
    
    async def _fetch_season_episodes(
        self,
        session: aiohttp.ClientSession,
        series_id: int,
        season_number: int
    ) -> List[Dict]:
        """
        Получить базовые данные эпизодов сезона.
        
        API: /tv/{series_id}/season/{season_number}
        Возвращает: episodes[] с name, overview, air_date, episode_number
        """
        data = await self.client._request(
            session,
            f"/tv/{series_id}/season/{season_number}",
            params={"language": "en"}
        )
        
        if not data:
            return []
        
        return data.get("episodes", [])
    
    async def _fetch_episode_translations(
        self,
        session: aiohttp.ClientSession,
        series_id: int,
        season_number: int,
        episode_number: int
    ) -> List[Dict]:
        """
        КРИТИЧНО: Получить ВСЕ переводы для одного эпизода
        
        API: /tv/{series_id}/season/{season_number}/episode/{episode_number}/translations
        
        Возвращает:
        {
            "translations": [
                {
                    "iso_639_1": "ru",
                    "data": {
                        "name": "Название эпизода",
                        "overview": "Описание эпизода"
                    }
                },
                ...
            ]
        }
        """
        data = await self.client._request(
            session,
            f"/tv/{series_id}/season/{season_number}/episode/{episode_number}/translations"
        )
        
        if not data:
            return []
        
        return data.get("translations", [])
    
    def _estimate_episodes_time(self, series_count: int) -> str:
        """Оценка времени с учетом загрузки переводов"""
        avg_seasons = 5
        avg_episodes_per_season = 10
        
        # Базовые запросы
        discover_time = (series_count / 20) / 45
        series_time = series_count / 45
        seasons_time = (series_count * avg_seasons) / 45
        
        # КРИТИЧНО: Переводы эпизодов
        total_episodes = series_count * avg_seasons * avg_episodes_per_season
        translations_time = total_episodes / 45
        
        total_minutes = (discover_time + series_time + seasons_time + translations_time) / 60
        
        return f"{total_minutes:.1f} minutes (~{total_episodes} episode translation requests)"
    
    def estimate_time(self) -> str:
        """Оценка времени загрузки"""
        if self.load_episodes:
            return self._estimate_episodes_time(self.target_count)
        else:
            # Без эпизодов
            discover_time = (self.target_count / 20) / 45
            series_time = self.target_count / 45
            total_minutes = (discover_time + series_time) / 60
            return f"{total_minutes:.1f} minutes (no episodes)"