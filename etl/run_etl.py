"""
ETL Orchestrator (ИСПРАВЛЕННЫЙ)
Правильный порядок: справочники → фильмы (API) → сериалы (API) → всё связано

Usage:
    # Справочники
    python etl/run_etl.py --stage dictionaries
    
    # Топ 10k фильмов через discover (быстро, ~4 минуты)
    python etl/run_etl.py --stage movies --movie-strategy discover --target-count 10000 --min-vote-count 500
    
    # Топ 50k фильмов через segmented (медленно, ~20 минут)
    python etl/run_etl.py --stage movies --movie-strategy discover-segmented --target-count 50000 --min-vote-count 100
    
    # Топ 500 сериалов БЕЗ эпизодов (быстро, ~2 минуты)
    python etl/run_etl.py --stage series --target-count 500 --min-vote-count 200
    
    # Топ 100 сериалов С эпизодами (медленно, ~5 минут)
    python etl/run_etl.py --stage series --target-count 100 --load-episodes --min-vote-count 500
    
    # Полный pipeline (справочники + 10k фильмов + 500 сериалов)
    python etl/run_etl.py --stage all --target-count 10000 --min-vote-count 500
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
    from loaders.movie_loader import MovieLoader  # ✅ ПРАВИЛЬНЫЙ ИМПОРТ
    from loaders.series_loader import SeriesLoader
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure all loader modules are in loaders/ directory")
    sys.exit(1)


class ETLOrchestrator:
    """
    Оркестратор ETL процессов.
    Управляет порядком выполнения и обрабатывает ошибки.
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
            
            # Логируем полный traceback
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
        min_vote_count: int = 500,
        **strategy_kwargs
    ):
        """
        Этап 2: Загрузка фильмов через API.
        
        Args:
            strategy: "discover" (топ 10k) или "discover-segmented" (топ 50k+)
            target_count: Количество фильмов
            min_vote_count: Минимум голосов для фильтрации
            **strategy_kwargs: Дополнительные параметры (year_from, sort_by и т.д.)
        """
        print("\n" + "="*70)
        print(f"STAGE 2: MOVIES")
        print(f"Strategy: {strategy}")
        print(f"Target: {target_count}")
        print(f"Min vote count: {min_vote_count}")
        print("="*70)
        
        return self.run_stage(
            f"Movies ({strategy})",
            lambda: MovieLoader(
                strategy=strategy,
                target_count=target_count,
                min_vote_count=min_vote_count,
                **strategy_kwargs
            ).run()
        )
    
    def run_series(
        self,
        strategy: str = "discover",
        target_count: int = 500,
        load_episodes: bool = False,
        min_vote_count: int = 100,
        **strategy_kwargs
    ):
        """
        Этап 3: Загрузка сериалов.
        
        Args:
            strategy: "discover"
            target_count: Количество сериалов
            load_episodes: Загружать ли эпизоды (МЕДЛЕННО, для MVP = False)
            min_vote_count: Минимум голосов для фильтрации
        """
        print("\n" + "="*70)
        print(f"STAGE 3: SERIES")
        print(f"Strategy: {strategy}")
        print(f"Target: {target_count} series")
        print(f"Min vote count: {min_vote_count}")
        print(f"Load episodes: {load_episodes}")
        print("="*70)
        
        if load_episodes:
            print("⚠️  WARNING: load_episodes=True will be VERY SLOW")
            print("   Consider loading only series + seasons for MVP")
        
        return self.run_stage(
            f"Series ({strategy})",
            lambda: SeriesLoader(
                strategy=strategy,
                target_count=target_count,
                load_episodes=load_episodes,
                min_vote_count=min_vote_count,
                **strategy_kwargs
            ).run()
        )
    
    def run_all(
        self,
        movie_strategy: str = "discover",
        movies_count: int = 10000,
        min_vote_count_movies: int = 500,
        series_count: int = 500,
        min_vote_count_series: int = 100,
        load_episodes: bool = False
    ):
        """Запуск всех этапов последовательно"""
        self.start_time = time.time()
        
        print("\n" + "🎬 "*35)
        print("FULL ETL PIPELINE STARTED")
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("🎬 "*35 + "\n")
        
        # Этап 1: Справочники (обязательно)
        if not self.run_dictionaries():
            print("\n❌ Critical error: Dictionaries stage failed")
            print("Cannot continue without reference data")
            return False
        
        # Этап 2: Фильмы
        if not self.run_movies(
            strategy=movie_strategy,
            target_count=movies_count,
            min_vote_count=min_vote_count_movies
        ):
            print("\n⚠️  Movies stage failed, but continuing...")
        
        # Этап 3: Сериалы
        if not self.run_series(
            target_count=series_count,
            load_episodes=load_episodes,
            min_vote_count=min_vote_count_series
        ):
            print("\n⚠️  Series stage failed, but continuing...")
        
        # Финальный отчет
        self._print_final_report()
        
        return len(self.errors) == 0
    
    def _print_final_report(self):
        """Печать финального отчета"""
        elapsed = time.time() - self.start_time
        
        print("\n" + "="*70)
        print("ETL PIPELINE COMPLETED")
        print("="*70)
        print(f"Total time: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")
        
        if self.errors:
            print(f"\n❌ Errors encountered: {len(self.errors)}")
            for i, error in enumerate(self.errors, 1):
                print(f"  {i}. {error}")
        else:
            print("\n✅ All stages completed successfully!")
        
        print("="*70 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="TMDB ETL Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Загрузить справочники
  python etl/run_etl.py --stage dictionaries
  
  # Топ 10k фильмов через discover (быстро)
  python etl/run_etl.py --stage movies --movie-strategy discover --target-count 10000 --min-vote-count 500
  
  # Топ 50k фильмов через segmented (медленно)
  python etl/run_etl.py --stage movies --movie-strategy discover-segmented --target-count 50000 --min-vote-count 100
  
  # Топ 500 сериалов БЕЗ эпизодов
  python etl/run_etl.py --stage series --target-count 500 --min-vote-count 200
  
  # Топ 100 сериалов С эпизодами
  python etl/run_etl.py --stage series --target-count 100 --load-episodes --min-vote-count 500
  
  # Полный pipeline
  python etl/run_etl.py --stage all --target-count 10000 --min-vote-count 500
        """
    )
    
    parser.add_argument(
        "--stage",
        choices=["all", "dictionaries", "movies", "series"],
        default="all",
        help="Which stage to run"
    )
    
    # Параметры для фильмов
    parser.add_argument(
        "--movie-strategy",
        choices=["discover", "discover-segmented"],
        default="discover",
        help="Strategy for loading movies"
    )
    
    parser.add_argument(
        "--target-count",
        type=int,
        default=10000,
        help="Target number of items (movies or series)"
    )
    
    parser.add_argument(
        "--min-vote-count",
        type=int,
        default=500,
        help="Minimum vote count for quality filtering"
    )
    
    # Параметры для сериалов
    parser.add_argument(
        "--load-episodes",
        action="store_true",
        help="Load episodes for series (SLOW, not recommended for MVP)"
    )
    
    args = parser.parse_args()
    
    orchestrator = ETLOrchestrator()
    
    if args.stage == "all":
        success = orchestrator.run_all(
            movie_strategy=args.movie_strategy,
            movies_count=args.target_count,
            min_vote_count_movies=args.min_vote_count,
            series_count=args.target_count,
            min_vote_count_series=args.min_vote_count,
            load_episodes=args.load_episodes
        )
    elif args.stage == "dictionaries":
        success = orchestrator.run_dictionaries()
    elif args.stage == "movies":
        success = orchestrator.run_movies(
            strategy=args.movie_strategy,
            target_count=args.target_count,
            min_vote_count=args.min_vote_count
        )
    elif args.stage == "series":
        success = orchestrator.run_series(
            strategy="discover",
            target_count=args.target_count,
            load_episodes=args.load_episodes,
            min_vote_count=args.min_vote_count
        )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()