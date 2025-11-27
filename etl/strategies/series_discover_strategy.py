"""
Стратегия загрузки сериалов через TMDB /discover/tv API.

КРИТИЧЕСКИЕ ОТЛИЧИЯ ОТ ФИЛЬМОВ:
1. Сериал = content + series_details + seasons + episodes
2. Каждый сезон может иметь 1-50 эпизодов
3. Нужна агрегация: сначала сериалы, потом сезоны, потом эпизоды

СТРАТЕГИЯ ЗАГРУЗКИ:
- Уровень 1: Загрузить топ N сериалов (как фильмы)
- Уровень 2: Для каждого сериала получить список сезонов
- Уровень 3: Для каждого сезона получить эпизоды (ОПЦИОНАЛЬНО для MVP)

ДЛЯ MVP: грузим только сериалы + сезоны, эпизоды позже
"""

import asyncio
from typing import List, Dict, Optional
from datetime import datetime
import aiohttp
from typing import List, Dict, Optional
import asyncio

class SeriesDiscoverStrategy:
    """
    Стратегия загрузки сериалов через /discover/tv.
    
    Параметры:
    - target_count: Количество сериалов (топ N)
    - min_vote_count: Минимум голосов (фильтр качества)
    - sort_by: Сортировка (popularity.desc, vote_average.desc)
    - load_episodes: Загружать ли эпизоды (МЕДЛЕННО, для MVP = False)
    """
    
    def __init__(
        self,
        client,
        target_count: int = 5000,
        sort_by: str = "popularity.desc",
        min_vote_count: int = 100,
        load_episodes: bool = False  # ДЛЯ MVP ОСТАВЬ False
    ):
        self.client = client
        self.target_count = target_count
        self.sort_by = sort_by
        self.min_vote_count = min_vote_count
        self.load_episodes = load_episodes
        
        # Лимиты TMDB
        self.max_pages = 500  # 10,000 results max
        self.results_per_page = 20
    
    async def get_series_ids(self) -> List[int]:
        """
        Получить список ID сериалов через /discover/tv.
        
        Аналогично фильмам, но эндпоинт другой.
        """
        import aiohttp
        
        print(f"📥 Discovering series (target: {self.target_count})...")
        
        series_ids = []
        pages_needed = min(
            (self.target_count + self.results_per_page - 1) // self.results_per_page,
            self.max_pages
        )
        
        async with aiohttp.ClientSession(headers=self.client.headers) as session:
            tasks = []
            
            for page in range(1, pages_needed + 1):
                tasks.append(self._fetch_discover_page(session, page))
                
                # Батчи по 50 страниц
                if len(tasks) >= 50 or page == pages_needed:
                    results = await asyncio.gather(*tasks)
                    
                    for page_data in results:
                        if page_data:
                            series_ids.extend([s["id"] for s in page_data.get("results", [])])
                    
                    tasks = []
                    
                    print(f"  Progress: {page}/{pages_needed} pages, {len(series_ids)} series found")
                    
                    if len(series_ids) >= self.target_count:
                        break
        
        # Обрезаем до target_count
        series_ids = series_ids[:self.target_count]
        
        print(f"✅ Discovered {len(series_ids)} series IDs")
        return series_ids
    
    async def _fetch_discover_page(self, session: aiohttp.ClientSession, page: int) -> Optional[Dict]:
        """Получить одну страницу discover results"""
        params = {
        "page": page,
        "sort_by": self.sort_by,
        "vote_count.gte": self.min_vote_count,
        "include_adult": "false"  # ← ИСПРАВЛЕНО: строка вместо bool
    }
        
        return await self.client._request(session, "/discover/tv", params)
    
    async def fetch_series_full_data(self, series_ids: List[int]) -> List[Dict]:
        """
        Загрузить полные данные сериалов.
        
        Для каждого сериала получаем:
        1. Базовые детали + translations
        2. Список сезонов (из aggregate_credits или TV details)
        3. Опционально: детали каждого эпизода (МЕДЛЕННО)
        """
        print(f"\n📥 Fetching full data for {len(series_ids)} series...")
        
        if self.load_episodes:
            print("⚠️  WARNING: load_episodes=True will be VERY SLOW")
            print(f"   Estimated time: {len(series_ids) * 5 * 0.02:.1f} minutes (assuming 5 seasons avg)")
        
        import aiohttp
        from tqdm.asyncio import tqdm_asyncio
        
        async with aiohttp.ClientSession(headers=self.client.headers) as session:
            # Загружаем базовые детали сериалов параллельно
            tasks = [
                self._fetch_series_with_seasons(session, series_id)
                for series_id in series_ids
            ]
            
            results = await tqdm_asyncio.gather(*tasks, desc="Fetching series")
            
            # Фильтруем None
            series_data = [r for r in results if r is not None]
            
            print(f"✅ Fetched {len(series_data)} series (with seasons)")
            
            # Если нужны эпизоды - загружаем их отдельно
            if self.load_episodes:
                await self._fetch_all_episodes(session, series_data)
            
            return series_data
    
    async def _fetch_series_with_seasons(
        self, 
        session: aiohttp.ClientSession, 
        series_id: int
    ) -> Optional[Dict]:
        """
        Получить сериал + его сезоны (БЕЗ эпизодов).
        
        TMDB API: /tv/{id}?append_to_response=translations
        Сезоны включены в базовый response.
        """
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
        
        # Добавляем пустой список эпизодов для каждого сезона
        # (заполним позже если load_episodes=True)
        for season in data.get("seasons", []):
            season["episodes"] = []
        
        return data
    
    async def _fetch_all_episodes(
        self, 
        session: aiohttp.ClientSession, 
        series_data: List[Dict]
    ):
        """
        МЕДЛЕННАЯ ОПЕРАЦИЯ: загрузить все эпизоды всех сезонов.
        
        Используй только для небольшого числа сериалов или после MVP.
        """
        print(f"\n📥 Fetching episodes for all seasons...")
        
        from tqdm.asyncio import tqdm_asyncio
        
        # Собираем все (series_id, season_number) пары
        season_tasks = []
        for series in series_data:
            series_id = series["id"]
            for season in series.get("seasons", []):
                season_number = season["season_number"]
                # Пропускаем "Specials" (season 0)
                if season_number > 0:
                    season_tasks.append((series_id, season_number, season))
        
        print(f"  Total seasons to fetch: {len(season_tasks)}")
        
        # Загружаем батчами
        batch_size = 100
        for i in range(0, len(season_tasks), batch_size):
            batch = season_tasks[i:i + batch_size]
            
            tasks = [
                self._fetch_season_episodes(session, series_id, season_num)
                for series_id, season_num, _ in batch
            ]
            
            results = await tqdm_asyncio.gather(
                *tasks, 
                desc=f"Fetching episodes batch {i//batch_size + 1}"
            )
            
            # Записываем эпизоды обратно в структуру данных
            for (series_id, season_num, season_obj), episodes in zip(batch, results):
                if episodes:
                    season_obj["episodes"] = episodes
    
    async def _fetch_season_episodes(
        self,
        session: aiohttp.ClientSession,
        series_id: int,
        season_number: int
    ) -> List[Dict]:
        """
        Получить все эпизоды сезона.
        
        TMDB API: /tv/{series_id}/season/{season_number}
        """
        data = await self.client._request(
            session,
            f"/tv/{series_id}/season/{season_number}",
            params={"language": "en"}
        )
        
        if not data:
            return []
        
        return data.get("episodes", [])
    
    def estimate_time(self) -> str:
        """Оценка времени загрузки"""
        # Discover: ~1 req на 20 сериалов
        discover_time = (self.target_count / 20) / 45  # seconds
        
        # Series details: 1 req на сериал
        series_time = self.target_count / 45  # seconds
        
        # Episodes: если включено
        episodes_time = 0
        if self.load_episodes:
            avg_seasons = 5  # среднее количество сезонов
            episodes_time = (self.target_count * avg_seasons) / 45  # seconds
        
        total_minutes = (discover_time + series_time + episodes_time) / 60
        
        return f"{total_minutes:.1f} minutes"


if __name__ == "__main__":
    # Тест оценки времени
    strategy = SeriesDiscoverStrategy(
        client=None,
        target_count=5000,
        load_episodes=False
    )
    print(f"Estimated time: {strategy.estimate_time()}")
    
    strategy_with_episodes = SeriesDiscoverStrategy(
        client=None,
        target_count=1000,
        load_episodes=True
    )
    print(f"With episodes: {strategy_with_episodes.estimate_time()}")