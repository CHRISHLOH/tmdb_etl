import requests
import time
import os
from typing import Optional, Dict, Any
from dotenv import load_dotenv

load_dotenv()

class TMDBClient:
    """
    TMDB API client с rate limiting и retry logic.
    Лимиты TMDB: 40 req/10s = 4 req/s = 250ms между запросами.
    """
    BASE_URL = "https://api.themoviedb.org/3"
    MIN_DELAY = 0.26  # 260ms между запросами для безопасности

    def __init__(self):
        self.bearer_token = os.getenv("TMDB_BEARER_TOKEN")
        if not self.bearer_token:
            raise ValueError("TMDB_BEARER_TOKEN not found in env")

        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.bearer_token}",
            "accept": "application/json"
        })
        self.last_request_time = 0

    def _wait_if_needed(self):
        """Rate limiting: ждем между запросами"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.MIN_DELAY:
            time.sleep(self.MIN_DELAY - elapsed)
        self.last_request_time = time.time()

    def _request(self, endpoint: str, params: Optional[Dict] = None, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """Базовый метод для всех запросов с retry"""
        url = f"{self.BASE_URL}{endpoint}"

        for attempt in range(max_retries):
            try:
                self._wait_if_needed()
                response = self.session.get(url, params=params, timeout=10)

                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:  # Rate limit hit
                    wait_time = int(response.headers.get('Retry-After', 2))
                    print(f"Rate limit hit, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                elif response.status_code == 404:
                    return None  # Не найдено - это нормально
                else:
                    print(f"TMDB error {response.status_code}: {url}")
                    return None

            except requests.RequestException as e:
                print(f"Request failed (attempt {attempt+1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff

        return None

    # ============ GENRES ============

    def get_genres(self, language: str = "en") -> list:
        """Получить список жанров"""
        data = self._request(f"/genre/movie/list", {"language": language})
        return data.get("genres", []) if data else []

    # ============ DISCOVER ============

    def discover_movies(self, page: int = 1, **filters) -> Dict:
        """
        Discover movies с фильтрами.
        Возвращает: {"results": [...], "total_pages": N, "total_results": M}
        """
        params = {"page": page, **filters}
        data = self._request("/discover/movie", params)
        return data or {"results": [], "total_pages": 0, "total_results": 0}

    # ============ MOVIE DETAILS ============

    def get_movie_details(self, movie_id: int, language: str = "en") -> Optional[Dict]:
        """Получить детали фильма"""
        return self._request(f"/movie/{movie_id}", {"language": language})

    def get_movie_translations(self, movie_id: int) -> list:
        """Получить все доступные переводы фильма"""
        data = self._request(f"/movie/{movie_id}/translations")
        return data.get("translations", []) if data else []

    def get_movie_credits(self, movie_id: int) -> Dict:
        """Получить cast & crew"""
        data = self._request(f"/movie/{movie_id}/credits")
        return data or {"cast": [], "crew": []}

    # ============ PERSON ============

    def get_person_details(self, person_id: int, language: str = "en") -> Optional[Dict]:
        """Получить детали персоны"""
        return self._request(f"/person/{person_id}", {"language": language})

    def get_person_translations(self, person_id: int) -> list:
        """Получить переводы биографии персоны"""
        data = self._request(f"/person/{person_id}/translations")
        return data.get("translations", []) if data else []

    # ============ CONFIGURATION ============

    def get_configuration(self) -> Dict:
        """Получить конфигурацию API (для построения URL изображений)"""
        return self._request("/configuration") or {}


if __name__ == "__main__":
    # Quick test
    client = TMDBClient()
    genres = client.get_genres()
    print(f"Found {len(genres)} genres")

    movies = client.discover_movies(page=1, with_original_language="en")
    print(f"Discovered {len(movies['results'])} movies")