"""
Стратегия загрузки через TMDB /discover/movie API.

Преимущества:
- Прямая фильтрация по качеству (popularity, vote_count, rating)
- Быстро: не нужно скачивать и парсить дампы
- Топ N фильмов (максимум 10,000 за один запрос)

Лимиты TMDB:
- 500 страниц × 20 фильмов = 10,000 max
"""

import asyncio
import aiohttp
from typing import List, Dict, Optional
from tqdm.asyncio import tqdm_asyncio


class DiscoverStrategy:
    """
    Загрузка топ N фильмов через /discover/movie.
    Максимум 10,000 фильмов.
    """
    
    BASE_URL = "https://api.themoviedb.org/3"
    MAX_PAGES = 500  # TMDB limit
    ITEMS_PER_PAGE = 20  # TMDB fixed
    
    def __init__(
        self,
        bearer_token: str,
        target_count: int = 10000,
        sort_by: str = "popularity.desc",
        min_vote_count: int = 200,
        min_vote_average: float = 0.0,
        exclude_adult: bool = True,
        exclude_video: bool = True
    ):
        """
        Args:
            bearer_token: TMDB API token
            target_count: Сколько фильмов загрузить (max 10,000)
            sort_by: Сортировка (popularity.desc, vote_average.desc, etc)
            min_vote_count: Минимальное количество голосов
            min_vote_average: Минимальный рейтинг (0-10)
            exclude_adult: Исключить adult content
            exclude_video: Исключить direct-to-video релизы
        """
        self.bearer_token = bearer_token
        self.target_count = min(target_count, self.MAX_PAGES * self.ITEMS_PER_PAGE)
        self.sort_by = sort_by
        self.min_vote_count = min_vote_count
        self.min_vote_average = min_vote_average
        self.exclude_adult = exclude_adult
        self.exclude_video = exclude_video
        
        self.headers = {
            "Authorization": f"Bearer {bearer_token}",
            "accept": "application/json"
        }
    
    def _get_discover_params(self, page: int) -> Dict:
        """Параметры для /discover/movie"""
        params = {
            "page": page,
            "sort_by": self.sort_by,
            "include_adult": "false" if self.exclude_adult else "true",
            "include_video": "false" if self.exclude_video else "true",
        }
        
        if self.min_vote_count > 0:
            params["vote_count.gte"] = self.min_vote_count
        
        if self.min_vote_average > 0:
            params["vote_average.gte"] = self.min_vote_average
        
        return params
    
    async def _fetch_page(
        self, 
        session: aiohttp.ClientSession, 
        page: int
    ) -> Optional[Dict]:
        """Загрузить одну страницу discover"""
        url = f"{self.BASE_URL}/discover/movie"
        params = self._get_discover_params(page)
        
        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:
                    retry_after = int(response.headers.get('Retry-After', 2))
                    print(f"⚠️  Rate limit, waiting {retry_after}s")
                    await asyncio.sleep(retry_after)
                    return await self._fetch_page(session, page)
                else:
                    print(f"❌ HTTP {response.status} for page {page}")
                    return None
        except Exception as e:
            print(f"❌ Error fetching page {page}: {e}")
            return None
    
    async def get_movie_ids(self) -> List[int]:
        """
        Получить список ID фильмов через discover API.
        
        Returns:
            List[int]: ID фильмов (до target_count)
        """
        pages_needed = min(
            (self.target_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE,
            self.MAX_PAGES
        )
        
        print(f"\n📥 Fetching {self.target_count} movies via /discover/movie")
        print(f"   Sort: {self.sort_by}")
        print(f"   Min votes: {self.min_vote_count}")
        print(f"   Min rating: {self.min_vote_average}")
        print(f"   Pages to fetch: {pages_needed}\n")
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            tasks = [
                self._fetch_page(session, page) 
                for page in range(1, pages_needed + 1)
            ]
            
            results = await tqdm_asyncio.gather(
                *tasks, 
                desc="Fetching discover pages"
            )
        
        # Собираем ID из всех страниц
        movie_ids = []
        for result in results:
            if result and "results" in result:
                for movie in result["results"]:
                    movie_ids.append(movie["id"])
                    if len(movie_ids) >= self.target_count:
                        break
            
            if len(movie_ids) >= self.target_count:
                break
        
        print(f"✅ Collected {len(movie_ids)} movie IDs")
        return movie_ids[:self.target_count]


# =============================================================================
# EXAMPLE USAGE
# =============================================================================

async def main():
    """Пример использования"""
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    token = os.getenv("TMDB_BEARER_TOKEN")
    
    # Топ 5000 популярных фильмов (минимум 500 голосов)
    strategy = DiscoverStrategy(
        bearer_token=token,
        target_count=5000,
        sort_by="popularity.desc",
        min_vote_count=500,
        min_vote_average=0.0
    )
    
    movie_ids = await strategy.get_movie_ids()
    print(f"\nПолучено {len(movie_ids)} фильмов")
    print(f"Примеры ID: {movie_ids[:10]}")


if __name__ == "__main__":
    asyncio.run(main())