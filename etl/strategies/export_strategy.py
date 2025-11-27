"""
Legacy стратегия через TMDB daily exports.

КОГДА ИСПОЛЬЗОВАТЬ:
- Тестирование на полном датасете (600k+ фильмов)
- Анализ coverage
- Загрузка специфичных фильмов (низкий popularity, но нужны)

ПРОБЛЕМЫ:
- 90% мусора (старые фильмы, короткометражки, video=true)
- Медленно (скачивание + парсинг дампа)
- Всё равно требует API запросов за деталями

ВЫВОД: использовать только для edge cases
"""

import requests
import gzip
import json
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional


class ExportStrategy:
    """
    Загрузка ID через TMDB daily export (NDJSON.gz файлы).
    """
    
    EXPORT_BASE_URL = "https://files.tmdb.org/p/exports"
    
    def __init__(
        self,
        target_count: int = 1000,
        min_popularity: float = 20.0,
        exclude_adult: bool = True,
        exclude_video: bool = True,
        data_dir: str = "./data"
    ):
        """
        Args:
            target_count: Сколько фильмов взять (топ N по popularity)
            min_popularity: Минимальная популярность
            exclude_adult: Исключить adult content
            exclude_video: Исключить direct-to-video
            data_dir: Куда скачивать файлы
        """
        self.target_count = target_count
        self.min_popularity = min_popularity
        self.exclude_adult = exclude_adult
        self.exclude_video = exclude_video
        self.data_dir = data_dir
        
        os.makedirs(data_dir, exist_ok=True)
    
    def _get_export_filename(self, days_back: int = 0) -> str:
        """Генерация имени файла export (формат: movie_ids_MM_DD_YYYY.json.gz)"""
        date = datetime.now() - timedelta(days=days_back)
        date_str = date.strftime("%m_%d_%Y")
        return f"movie_ids_{date_str}.json.gz"
    
    def _download_export(self, max_retries: int = 7) -> Optional[str]:
        """
        Скачать последний доступный export файл.
        
        Returns:
            Путь к скачанному файлу или None
        """
        print("\n📥 Downloading TMDB daily export...")
        
        for days_back in range(max_retries):
            filename = self._get_export_filename(days_back)
            url = f"{self.EXPORT_BASE_URL}/{filename}"
            local_path = os.path.join(self.data_dir, filename)
            
            # Проверяем кэш
            if os.path.exists(local_path):
                print(f"✅ Using cached file: {filename}")
                return local_path
            
            print(f"  Trying: {url}")
            
            try:
                response = requests.get(url, stream=True, timeout=30)
                
                if response.status_code == 200:
                    file_size = int(response.headers.get('content-length', 0))
                    
                    print(f"  Downloading {filename} ({file_size / 1024 / 1024:.2f} MB)...")
                    
                    with open(local_path, 'wb') as f:
                        downloaded = 0
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                            downloaded += len(chunk)
                            
                            if file_size > 0:
                                percent = (downloaded / file_size) * 100
                                print(f"\r    Progress: {percent:.1f}%", end='', flush=True)
                    
                    print(f"\n✅ Downloaded: {filename}")
                    return local_path
                
                elif response.status_code == 403:
                    print(f"  Access denied (file not available yet)")
                
            except requests.RequestException as e:
                print(f"  Download failed: {e}")
        
        print(f"❌ Could not download export after {max_retries} attempts")
        return None
    
    def _parse_export(self, filepath: str) -> List[Dict]:
        """
        Парсинг NDJSON export с фильтрацией.
        
        Returns:
            List[Dict]: Отфильтрованные записи с полями {id, popularity, ...}
        """
        print(f"\n⚙️  Parsing {os.path.basename(filepath)}...")
        
        results = []
        
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    item = json.loads(line.strip())
                    
                    # Фильтры
                    if self.exclude_adult and item.get("adult", False):
                        continue
                    
                    if self.exclude_video and item.get("video", False):
                        continue
                    
                    popularity = item.get("popularity", 0)
                    if popularity < self.min_popularity:
                        continue
                    
                    results.append(item)
                
                except json.JSONDecodeError:
                    continue
        
        print(f"✅ Parsed {len(results)} valid items (from ~{line_num} total)")
        return results
    
    def get_movie_ids(self) -> List[int]:
        """
        Полный цикл: скачать → распарсить → отфильтровать → вернуть топ N.
        
        Returns:
            List[int]: ID фильмов (топ N по popularity)
        """
        # 1. Скачать export
        filepath = self._download_export()
        if not filepath:
            raise RuntimeError("Failed to download daily export")
        
        # 2. Парсить и фильтровать
        items = self._parse_export(filepath)
        
        # 3. Сортировать по popularity (desc)
        items.sort(key=lambda x: x.get("popularity", 0), reverse=True)
        
        # 4. Взять топ N
        items = items[:self.target_count]
        
        # 5. Извлечь ID
        movie_ids = [item["id"] for item in items]
        
        print(f"✅ Selected top {len(movie_ids)} movies by popularity")
        return movie_ids


# =============================================================================
# EXAMPLE USAGE
# =============================================================================

def main():
    """Пример: топ 1000 из export"""
    strategy = ExportStrategy(
        target_count=1000,
        min_popularity=20.0,
        exclude_adult=True,
        exclude_video=True
    )
    
    movie_ids = strategy.get_movie_ids()
    print(f"\nПолучено {len(movie_ids)} фильмов")
    print(f"Примеры ID: {movie_ids[:10]}")


if __name__ == "__main__":
    main()