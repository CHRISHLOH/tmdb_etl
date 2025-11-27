"""
Стратегия обхода лимита 10k через сегментацию по годам.

Как работает:
1. Делим период на годы (например 1990-2024)
2. Для каждого года делаем отдельный discover запрос
3. Каждый год может вернуть до 10,000 фильмов
4. Итого: 35 лет × 10k = 350,000 теоретически

Реально:
- Современные годы (2020+): ~5,000-8,000 фильмов/год
- Старые годы (1990-2000): ~2,000-4,000 фильмов/год
- Очень старые (<1980): <1,000 фильмов/год

Итого реально: 50,000-150,000 качественных фильмов
"""

import asyncio
import aiohttp
from typing import List, Dict, Optional, Set
from tqdm.asyncio import tqdm_asyncio
from datetime import datetime


class DiscoverSegmentedStrategy:
    """
    Загрузка >10k фильмов через сегментацию по годам.
    """
    
    BASE_URL = "https://api.themoviedb.org/3"
    MAX_PAGES = 500
    ITEMS_PER_PAGE = 20
    
    def __init__(
        self,
        bearer_token: str,
        target_count: int = 50000,
        year_from: int = 1990,
        year_to: Optional[int] = None,
        sort_by: str = "popularity.desc",
        min_vote_count: int = 100,
        exclude_adult: bool = True,
        exclude_video: bool = True
    ):
        """
        Args:
            bearer_token: TMDB API token
            target_count: Целевое количество (может быть >10k)
            year_from: С какого года грузить
            year_to: До какого года (None = текущий год)
            sort_by: Сортировка внутри года
            min_vote_count: Минимум голосов (для фильтрации мусора)
        """
        self.bearer_token = bearer_token
        self.target_count = target_count
        self.year_from = year_from
        self.year_to = year_to or datetime.now().year
        self.sort_by = sort_by
        self.min_vote_count = min_vote_count
        self.exclude_adult = exclude_adult
        self.exclude_video = exclude_video
        
        self.headers = {
            "Authorization": f"Bearer {bearer_token}",
            "accept": "application/json"
        }
        
        # Для дедупликации (фильмы могут быть в нескольких годах)
        self.seen_ids: Set[int] = set()
    
    def _get_year_segments(self) -> List[int]:
        """Генерация списка годов для обработки"""
        return list(range(self.year_to, self.year_from - 1, -1))  # Новые первыми
    
    def _get_discover_params(self, year: int, page: int) -> Dict:
        """Параметры discover для конкретного года"""
        params = {
            "page": page,
            "sort_by": self.sort_by,
            "primary_release_year": year,  # КЛЮЧЕВОЙ ПАРАМЕТР для сегментации
            "include_adult": "false" if self.exclude_adult else "true",
            "include_video": "false" if self.exclude_video else "true",
        }
        
        if self.min_vote_count > 0:
            params["vote_count.gte"] = self.min_vote_count
        
        return params
    
    async def _fetch_page(
        self,
        session: aiohttp.ClientSession,
        year: int,
        page: int
    ) -> Optional[Dict]:
        """Загрузить одну страницу для года"""
        url = f"{self.BASE_URL}/discover/movie"
        params = self._get_discover_params(year, page)
        
        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:
                    retry_after = int(response.headers.get('Retry-After', 2))
                    await asyncio.sleep(retry_after)
                    return await self._fetch_page(session, year, page)
                else:
                    return None
        except Exception as e:
            print(f"❌ Error: year={year}, page={page}: {e}")
            return None
    
    async def _fetch_year(
        self,
        session: aiohttp.ClientSession,
        year: int,
        max_per_year: int
    ) -> List[int]:
        """
        Загрузить фильмы для одного года.
        
        Args:
            year: Год релиза
            max_per_year: Максимум фильмов из этого года
        
        Returns:
            List[int]: ID фильмов
        """
        # Первый запрос чтобы узнать total_pages
        first_page = await self._fetch_page(session, year, 1)
        
        if not first_page:
            return []
        
        total_pages = min(first_page.get("total_pages", 1), self.MAX_PAGES)
        pages_needed = min(
            (max_per_year + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE,
            total_pages
        )
        
        # Загружаем остальные страницы
        if pages_needed > 1:
            tasks = [
                self._fetch_page(session, year, page)
                for page in range(2, pages_needed + 1)
            ]
            other_pages = await asyncio.gather(*tasks)
            all_pages = [first_page] + [p for p in other_pages if p]
        else:
            all_pages = [first_page]
        
        # Собираем ID
        year_ids = []
        for page_data in all_pages:
            if page_data and "results" in page_data:
                for movie in page_data["results"]:
                    movie_id = movie["id"]
                    
                    # Дедупликация
                    if movie_id not in self.seen_ids:
                        self.seen_ids.add(movie_id)
                        year_ids.append(movie_id)
                    
                    if len(year_ids) >= max_per_year:
                        break
            
            if len(year_ids) >= max_per_year:
                break
        
        return year_ids
    
    async def get_movie_ids(self) -> List[int]:
        """
        Получить список ID через сегментацию по годам.
        
        Returns:
            List[int]: ID фильмов (до target_count)
        """
        years = self._get_year_segments()
        
        print(f"\n📥 Fetching {self.target_count} movies via year segmentation")
        print(f"   Years: {self.year_from} - {self.year_to} ({len(years)} years)")
        print(f"   Sort: {self.sort_by}")
        print(f"   Min votes: {self.min_vote_count}")
        print(f"   Strategy: ~{self.target_count // len(years)} movies/year\n")
        
        movie_ids = []
        movies_per_year = (self.target_count + len(years) - 1) // len(years)
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            # Обрабатываем года последовательно (для контроля rate limit)
            for year in tqdm_asyncio(years, desc="Processing years"):
                if len(movie_ids) >= self.target_count:
                    break
                
                year_ids = await self._fetch_year(
                    session, 
                    year, 
                    min(movies_per_year, self.target_count - len(movie_ids))
                )
                
                movie_ids.extend(year_ids)
                
                print(f"  {year}: +{len(year_ids)} movies (total: {len(movie_ids)})")
        
        print(f"\n✅ Collected {len(movie_ids)} unique movie IDs")
        return movie_ids[:self.target_count]


# =============================================================================
# EXAMPLE USAGE
# =============================================================================

async def main():
    """Пример: топ 50k фильмов за 1990-2024"""
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    token = os.getenv("TMDB_BEARER_TOKEN")
    
    strategy = DiscoverSegmentedStrategy(
        bearer_token=token,
        target_count=50000,
        year_from=1990,
        year_to=2024,
        sort_by="popularity.desc",
        min_vote_count=100
    )
    
    movie_ids = await strategy.get_movie_ids()
    print(f"\nПолучено {len(movie_ids)} уникальных фильмов")


if __name__ == "__main__":
    asyncio.run(main())