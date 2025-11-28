"""
TMDB API клиенты: синхронный и асинхронный (ФИНАЛЬНОЕ ИСПРАВЛЕНИЕ)
"""

import requests
import asyncio
import aiohttp
import time
import os
from typing import List, Dict, Optional
from tqdm.asyncio import tqdm_asyncio
from dotenv import load_dotenv

load_dotenv()


# ============================================================================
# СИНХРОННЫЙ КЛИЕНТ (для справочников и старого кода)
# ============================================================================

class TMDBClient:
    """
    Синхронный TMDB API client с rate limiting.
    Используется для справочников (genres, countries, languages).
    """
    
    BASE_URL = "https://api.themoviedb.org/3"
    MIN_DELAY = 0.11  # 110ms = ~9 req/s
    
    def __init__(self):
        self.bearer_token = os.getenv("TMDB_BEARER_TOKEN")
        if not self.bearer_token:
            raise ValueError("TMDB_BEARER_TOKEN not found in .env")
        
        self.headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "accept": "application/json"
        }
        self.last_request_time = 0
    
    def _rate_limit(self):
        """Простой rate limiter"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.MIN_DELAY:
            time.sleep(self.MIN_DELAY - elapsed)
        self.last_request_time = time.time()
    
    def _request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Базовый запрос с retry logic"""
        self._rate_limit()
        
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=10)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 2))
                print(f"⚠️  Rate limit hit, waiting {retry_after}s")
                time.sleep(retry_after)
                return self._request(endpoint, params)
            elif response.status_code == 404:
                return None
            else:
                print(f"❌ HTTP {response.status_code}: {url}")
                return None
        
        except requests.RequestException as e:
            print(f"❌ Request failed: {e}")
            return None
    
    def get_configuration(self) -> Optional[Dict]:
        """Получить конфигурацию TMDB"""
        return self._request("/configuration")
    
    def get_genres(self, media_type: str = "movie", language: str = "en") -> List[Dict]:
        """Получить список жанров"""
        result = self._request(f"/genre/{media_type}/list", params={"language": language})
        return result.get("genres", []) if result else []
    
    def get_movie_details(self, movie_id: int, language: str = "en") -> Optional[Dict]:
        """Получить детали фильма"""
        return self._request(f"/movie/{movie_id}", params={"language": language})
    
    def get_movie_translations(self, movie_id: int) -> List[Dict]:
        """Получить переводы фильма"""
        result = self._request(f"/movie/{movie_id}/translations")
        return result.get("translations", []) if result else []


# ============================================================================
# АСИНХРОННЫЙ КЛИЕНТ (для массовой загрузки)
# ============================================================================

class AsyncTMDBClient:
    """
    Асинхронный TMDB клиент с параллельными запросами.
    
    ФИНАЛЬНОЕ ИСПРАВЛЕНИЕ: семафор пересоздается для каждого нового event loop
    
    Лимиты TMDB:
    - 50 requests/second
    - 20 одновременных соединений на IP
    
    Стратегия: используем 18 параллельных соединений и ~45 req/s
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
        
        # НЕ создаем семафор здесь!
        self._semaphore = None
        self._loop_id = None  # ID текущего event loop
        self.request_times = []
    
    def _get_semaphore(self):
        """
        Получить семафор для текущего event loop.
        Если event loop изменился - создаем новый семафор.
        """
        try:
            current_loop = asyncio.get_running_loop()
            current_loop_id = id(current_loop)
            
            # Если это новый event loop - создаем новый семафор
            if self._loop_id != current_loop_id:
                self._semaphore = asyncio.Semaphore(self.MAX_CONCURRENT)
                self._loop_id = current_loop_id
            
            return self._semaphore
        except RuntimeError:
            # Нет running loop - вернем None, создадим позже
            return None
    
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
        
        # Получаем семафор для текущего event loop
        semaphore = self._get_semaphore()
        
        async with semaphore:
            await self._rate_limit()
            
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

    
    async def get_person_full_data(
        self, 
        session: aiohttp.ClientSession, 
        person_id: int
    ) -> Optional[Dict]:
        """Получить полные данные персоны"""
        return await self._request(
            session,
            f"/person/{person_id}",
            params={
                "language": "en",
                "append_to_response": "translations,combined_credits"
            }
        )
    
    async def fetch_persons_batch(
        self, 
        person_ids: List[int],
        progress_desc: str = "Fetching persons"
    ) -> List[Dict]:
        """
        Загрузить батч персон параллельно.
        
        Использует 18 параллельных соединений и ~45 req/s.
        """
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            tasks = [
                self.get_person_full_data(session, person_id) 
                for person_id in person_ids
            ]
            
            # Выполняем с прогресс-баром
            results = await tqdm_asyncio.gather(*tasks, desc=progress_desc)
            
            # Фильтруем None (не найденные персоны)
            return [r for r in results if r is not None]

# ============================================================================
# УТИЛИТЫ
# ============================================================================

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
    asyncio.run(benchmark_api())