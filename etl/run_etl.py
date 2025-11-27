"""
ETL Orchestrator с поддержкой стратегий загрузки фильмов.

Usage:
    # Справочники
    python run_etl.py --stage dictionaries
    
    # Фильмы через разные стратегии:
    
    # 1. Discover (топ 10k, быстро)
    python run_etl.py --stage movies --strategy discover --target-count 10000
    
    # 2. Discover Segmented (топ 50k, сегментация по годам)
    python run_etl.py --stage movies --strategy discover-segmented --target-count 50000 --year-from 1990
    
    # 3. Export (legacy, через daily dump)
    python run_etl.py --stage movies --strategy export --target-count 1000 --min-popularity 20
    
    # Полный пайплайн
    python run_etl.py --stage all --strategy discover-segmented --target-count 30000
"""

import argparse
import sys
import time
from datetime import datetime

# Импорты загрузчиков
try:
    from loaders.genre_loader import GenreLoader
    from loaders.country_loader import CountryLoader
    from loaders.language_loader import LanguageLoader
    from loaders.movie_loader import MovieLoader  # НОВЫЙ РЕФАКТОРЕННЫЙ
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure all loader modules are in loaders/ directory")
    sys.exit(1)


class ETLOrchestrator:
    """
    Оркестратор ETL процессов с поддержкой стратегий.
    """
    
    def __init__(self):
        self.start_time = None
        self.errors = []
    
    def run_stage(self, stage_name: str, loader_func):
        """Запуск одного этапа с обработкой ошибок"""
        print(f"\n{'🚀 '*30}")
        print(f"STAGE: {stage_name}")
        print(f"{'🚀 '*30}\n")
        
        try:
            loader_func()
            print(f"✅ Stage '{stage_name}' completed successfully")
            return True
        except Exception as e:
            error_msg = f"Stage '{stage_name}' failed: {str(e)}"
            print(f"❌ {error_msg}")
            self.errors.append(error_msg)
            
            import traceback
            traceback.print_exc()
            
            return False
    
    def run_dictionaries(self):
        """Этап 1: Загрузка справочников"""
        print("\n" + "="*70)
        print("STAGE 1: DICTIONARIES")
        print("="*70)
        
        stages = [
            ("Genres", lambda: GenreLoader().run()),
            ("Countries", lambda: CountryLoader().run()),
            ("Languages", lambda: LanguageLoader().run()),
        ]
        
        success_count = 0
        for name, loader_func in stages:
            if self.run_stage(name, loader_func):
                success_count += 1
        
        print(f"\n📊 Dictionaries stage: {success_count}/{len(stages)} successful")
        return success_count == len(stages)
    
    def run_movies(
        self,
        strategy: str = "discover",
        target_count: int = 10000,
        **strategy_kwargs
    ):
        """
        Этап 2: Загрузка фильмов с выбранной стратегией.
        
        Args:
            strategy: "discover", "discover-segmented", или "export"
            target_count: Количество фильмов
            **strategy_kwargs: Параметры для стратегии
        """
        print("\n" + "="*70)
        print(f"STAGE 2: MOVIES")
        print(f"Strategy: {strategy}")
        print(f"Target: {target_count} movies")
        print("="*70)
        
        # Описание стратегий
        strategy_info = {
            "discover": "Fast: Top 10k via /discover (no segmentation)",
            "discover-segmented": "Medium: 50k+ via year segmentation",
            "export": "Legacy: via daily export dump (slow)"
        }
        
        print(f"📝 {strategy_info.get(strategy, 'Unknown strategy')}\n")
        
        return self.run_stage(
            f"Movies ({strategy})",
            lambda: MovieLoader(
                strategy=strategy,
                target_count=target_count,
                **strategy_kwargs
            ).run()
        )
    
    def run_all(
        self,
        strategy: str = "discover",
        target_count: int = 10000,
        **strategy_kwargs
    ):
        """Запуск всех этапов"""
        self.start_time = time.time()
        
        print("\n" + "🎬 "*35)
        print("FULL ETL PIPELINE STARTED")
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Strategy: {strategy}")
        print(f"Target: {target_count} movies")
        print("🎬 "*35 + "\n")
        
        # Этап 1: Справочники
        if not self.run_dictionaries():
            print("\n❌ Critical error: Dictionaries stage failed")
            print("Cannot continue without reference data")
            return False
        
        # Этап 2: Фильмы
        if not self.run_movies(strategy, target_count, **strategy_kwargs):
            print("\n⚠️  Movies stage failed")
        
        # Финальный отчет
        self._print_final_report()
        
        return len(self.errors) == 0
    
    def _print_final_report(self):
        """Печать финального отчета"""
        elapsed = time.time() - self.start_time
        
        print("\n" + "="*70)
        print("ETL PIPELINE COMPLETED")
        print("="*70)
        print(f"Total time: {elapsed:.2f}s ({elapsed/60:.2f} minutes)")
        
        if self.errors:
            print(f"\n❌ Errors: {len(self.errors)}")
            for i, error in enumerate(self.errors, 1):
                print(f"  {i}. {error}")
        else:
            print("\n✅ All stages completed successfully!")
        
        print("="*70 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="TMDB ETL Orchestrator with Strategy Support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Топ 10,000 популярных (быстро)
  python run_etl.py --stage movies --strategy discover --target-count 10000
  
  # Топ 50,000 через сегментацию (1990-2024)
  python run_etl.py --stage movies --strategy discover-segmented --target-count 50000 --year-from 1990
  
  # Legacy через daily export
  python run_etl.py --stage movies --strategy export --target-count 1000 --min-popularity 20
  
  # Полный пайплайн
  python run_etl.py --stage all --strategy discover-segmented --target-count 30000
        """
    )
    
    parser.add_argument(
        "--stage",
        choices=["all", "dictionaries", "movies"],
        default="all",
        help="Which stage to run"
    )
    
    parser.add_argument(
        "--strategy",
        choices=["discover", "discover-segmented", "export"],
        default="discover",
        help="Movie loading strategy"
    )
    
    parser.add_argument(
        "--target-count",
        type=int,
        default=10000,
        help="Number of movies to load"
    )
    
    # Параметры для discover-segmented
    parser.add_argument(
        "--year-from",
        type=int,
        default=1990,
        help="Start year for segmented strategy (default: 1990)"
    )
    
    parser.add_argument(
        "--year-to",
        type=int,
        default=None,
        help="End year for segmented strategy (default: current year)"
    )
    
    # Параметры для discover/discover-segmented
    parser.add_argument(
        "--sort-by",
        default="popularity.desc",
        help="Sort order (popularity.desc, vote_average.desc, etc)"
    )
    
    parser.add_argument(
        "--min-vote-count",
        type=int,
        default=100,
        help="Minimum vote count filter"
    )
    
    # Параметры для export
    parser.add_argument(
        "--min-popularity",
        type=float,
        default=20.0,
        help="Minimum popularity for export strategy"
    )
    
    args = parser.parse_args()
    
    # Подготовка kwargs для стратегии
    strategy_kwargs = {}
    
    if args.strategy in ["discover", "discover-segmented"]:
        strategy_kwargs["sort_by"] = args.sort_by
        strategy_kwargs["min_vote_count"] = args.min_vote_count
        
        if args.strategy == "discover-segmented":
            strategy_kwargs["year_from"] = args.year_from
            if args.year_to:
                strategy_kwargs["year_to"] = args.year_to
    
    elif args.strategy == "export":
        strategy_kwargs["min_popularity"] = args.min_popularity
    
    # Запуск
    orchestrator = ETLOrchestrator()
    
    if args.stage == "all":
        success = orchestrator.run_all(
            strategy=args.strategy,
            target_count=args.target_count,
            **strategy_kwargs
        )
    elif args.stage == "dictionaries":
        success = orchestrator.run_dictionaries()
    elif args.stage == "movies":
        success = orchestrator.run_movies(
            strategy=args.strategy,
            target_count=args.target_count,
            **strategy_kwargs
        )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()