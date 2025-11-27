"""
ETL стратегии для загрузки фильмов из TMDB.

Доступные стратегии:
1. DiscoverStrategy - Топ N фильмов через /discover/movie
2. DiscoverSegmentedStrategy - Обход лимита 10k через сегментацию по годам
3. ExportStrategy - Legacy загрузка через daily export
"""

from .discover_strategy import DiscoverStrategy
from .discover_segmented_strategy import DiscoverSegmentedStrategy
from .export_strategy import ExportStrategy

__all__ = [
    'DiscoverStrategy',
    'DiscoverSegmentedStrategy', 
    'ExportStrategy'
]