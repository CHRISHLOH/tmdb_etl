"""
ETL Orchestrator (УЛУЧШЕННЫЙ)
Поддержка гибкой настройки параметров для фильмов и сериалов отдельно

Usage:
    # Полный pipeline с максимальным качеством одной командой
    python etl/run_etl.py --stage all \
        --movie-strategy discover-segmented \
        --movies-count 50000 \
        --movies-min-votes 500 \
        --series-count 1000 \
        --series-min-votes 500 \
        --load-episodes
    
    # Справочники
    python etl/run_etl.py --stage dictionaries
    
    # Только фильмы (топ 50k)
    python etl/run_etl.py --stage movies \
        --movie-strategy discover-segmented \
        --movies-count 50000 \
        --movies-min-votes 500
    
    # Только сериалы с эпизодами (топ 1000)
    python etl/run_etl.py --stage series \
        --series-count 1000 \
        --series-min-votes 500 \
        --load-episodes
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
    from loaders.career_loader import CareerLoader
    from loaders.movie_loader import MovieLoader
    from loaders.series_loader import SeriesLoader
    from loaders.person_loader import PersonLoader
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure all loader modules are in loaders/ directory")
    sys.exit(1)


class ETLOrchestrator:
    """
    Оркестратор ETL процессов с расширенными параметрами.
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
            ("Careers", lambda: CareerLoader().run()),
        ]
        
        success_count = 0
        for name, loader_func in stages:
            if self.run_stage(name, loader_func):
                success_count += 1
        
        print(f"\n📊 Dictionaries stage: {success_count}/{len(stages)} successful")
        return success_count == len(stages)
    
    def run_movies(
        self,
        strategy: str = "discover-segmented",
        target_count: int = 50000,
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
        print(f"Target: {target_count:,}")
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
    
    def run_persons(
        self,
        target_count: int = 5000,
        min_popularity: float = 5.0,
        load_from_content: bool = True
    ):
        """
        Этап 4: Загрузка персон.
    
        Args:
        target_count: Количество персон
        min_popularity: Минимальная популярность
        load_from_content: Собирать персон из уже загруженного контента
        """
        print("\n" + "="*70)
        print(f"STAGE 4: PERSONS")
        print(f"Target: {target_count:,} persons")
        print(f"Min popularity: {min_popularity}")
        print(f"Load from content: {load_from_content}")
        print("="*70)
    
        return self.run_stage(
            "Persons",
            lambda: PersonLoader(
            target_count=target_count,
            min_popularity=min_popularity,
            load_from_content=load_from_content
        ).run()
    )

    def run_series(
        self,
        strategy: str = "discover",
        target_count: int = 1000,
        load_episodes: bool = True,
        min_vote_count: int = 500,
        **strategy_kwargs
    ):
        """
        Этап 3: Загрузка сериалов.
    
        Args:
            strategy: "discover"
            target_count: Количество сериалов
            load_episodes: Загружать ли эпизоды
            min_vote_count: Минимум голосов для фильтрации
        """
        print("\n" + "="*70)
        print(f"STAGE 3: SERIES")
        print(f"Strategy: {strategy}")
        print(f"Target: {target_count:,} series")
        print(f"Min vote count: {min_vote_count}")
        print(f"Load episodes: {load_episodes}")
        print("="*70)
    
        if load_episodes:
            avg_seasons = 5
            estimated_minutes = (target_count * avg_seasons) / 45 / 60
            print(f"⏱️  Estimated time with episodes: ~{estimated_minutes:.1f} minutes")
    
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
        # Movies parameters
        movie_strategy: str = "discover-segmented",
        movies_count: int = 50000,
        movies_min_votes: int = 500,
        # Series parameters
        series_count: int = 1000,
        series_min_votes: int = 500,
        load_episodes: bool = True,
        # Persons parameters
        persons_count: int = 5000,
        persons_min_popularity: float = 5.0,
        load_persons: bool = True
    ):
        """Запуск всех этапов последовательно с отдельными параметрами"""
        self.start_time = time.time()
        
        print("\n" + "🎬 "*35)
        print("FULL ETL PIPELINE STARTED")
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        print(f"Movies: {movies_count:,} (min votes: {movies_min_votes}, strategy: {movie_strategy})")
        print(f"Series: {series_count:,} (min votes: {series_min_votes}, episodes: {load_episodes})")
        print(f"Persons: {persons_count:,} (min popularity: {persons_min_popularity}, enabled: {load_persons})")
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
            min_vote_count=movies_min_votes
        ):
            print("\n⚠️  Movies stage failed, but continuing...")
        
        # Этап 3: Сериалы
        if not self.run_series(
            target_count=series_count,
            load_episodes=load_episodes,
            min_vote_count=series_min_votes
        ):
            print("\n⚠️  Series stage failed, but continuing...")
        
        # Этап 4: Персоны (если включено)
        if load_persons:
            if not self.run_persons(
                target_count=persons_count,
                min_popularity=persons_min_popularity,
                load_from_content=True
            ):
                print("\n⚠️  Persons stage failed, but continuing...")
        
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
        description="TMDB ETL Orchestrator (Enhanced)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Полный pipeline с максимальным качеством (50k фильмов + 1k сериалов с эпизодами)
  python etl/run_etl.py --stage all \\
      --movie-strategy discover-segmented \\
      --movies-count 50000 \\
      --movies-min-votes 500 \\
      --series-count 1000 \\
      --series-min-votes 500 \\
      --load-episodes
  
  # Загрузить справочники
  python etl/run_etl.py --stage dictionaries
  
  # Топ 50k фильмов через segmented (высокое качество)
  python etl/run_etl.py --stage movies \\
      --movie-strategy discover-segmented \\
      --movies-count 50000 \\
      --movies-min-votes 500
  
  # Топ 1000 сериалов С эпизодами
  python etl/run_etl.py --stage series \\
      --series-count 1000 \\
      --series-min-votes 500 \\
      --load-episodes
        """
    )
    
    # Общие параметры
    parser.add_argument(
        "--stage",
        choices=["all", "dictionaries", "movies", "series", "persons"],
        default="all",
        help="Which stage to run"
    )
    
    # ========================================================================
    # ПАРАМЕТРЫ ДЛЯ ФИЛЬМОВ
    # ========================================================================
    parser.add_argument(
        "--movie-strategy",
        choices=["discover", "discover-segmented"],
        default="discover-segmented",
        help="Strategy for loading movies (default: discover-segmented for >10k)"
    )
    
    parser.add_argument(
        "--movies-count",
        type=int,
        default=50000,
        help="Target number of movies (default: 50000)"
    )
    
    parser.add_argument(
        "--movies-min-votes",
        type=int,
        default=500,
        help="Minimum vote count for movies (default: 500)"
    )
    
    # ========================================================================
    # ПАРАМЕТРЫ ДЛЯ СЕРИАЛОВ
    # ========================================================================
    parser.add_argument(
        "--series-count",
        type=int,
        default=1000,
        help="Target number of series (default: 1000)"
    )
    
    parser.add_argument(
        "--series-min-votes",
        type=int,
        default=500,
        help="Minimum vote count for series (default: 500)"
    )
    
    parser.add_argument(
        "--load-episodes",
        action="store_true",
        help="Load episodes for series (slower, but complete data)"
    )
    
    # ========================================================================
    # ПАРАМЕТРЫ ДЛЯ ПЕРСОН
    # ========================================================================
    parser.add_argument(
        "--persons-count",
        type=int,
        default=5000,
        help="Target number of persons (default: 5000)"
    )
    
    parser.add_argument(
        "--persons-min-popularity",
        type=float,
        default=5.0,
        help="Minimum popularity for persons (default: 5.0)"
    )
    
    parser.add_argument(
        "--load-persons",
        action="store_true",
        default=True,
        help="Load persons data (default: True)"
    )
    
    # ========================================================================
    # LEGACY ПАРАМЕТР (для обратной совместимости)
    # ========================================================================
    parser.add_argument(
        "--target-count",
        type=int,
        help="[DEPRECATED] Use --movies-count and --series-count instead"
    )
    
    parser.add_argument(
        "--min-vote-count",
        type=int,
        help="[DEPRECATED] Use --movies-min-votes and --series-min-votes instead"
    )
    
    args = parser.parse_args()
    
    # Обратная совместимость со старыми параметрами
    if args.target_count:
        print("⚠️  --target-count is deprecated, use --movies-count and --series-count")
        args.movies_count = args.target_count
        args.series_count = args.target_count
    
    if args.min_vote_count:
        print("⚠️  --min-vote-count is deprecated, use --movies-min-votes and --series-min-votes")
        args.movies_min_votes = args.min_vote_count
        args.series_min_votes = args.min_vote_count
    
    orchestrator = ETLOrchestrator()
    
    if args.stage == "all":
        success = orchestrator.run_all(
            movie_strategy=args.movie_strategy,
            movies_count=args.movies_count,
            movies_min_votes=args.movies_min_votes,
            series_count=args.series_count,
            series_min_votes=args.series_min_votes,
            load_episodes=args.load_episodes,
            persons_count=args.persons_count,
            persons_min_popularity=args.persons_min_popularity,
            load_persons=args.load_persons
        )
    elif args.stage == "dictionaries":
        success = orchestrator.run_dictionaries()
    elif args.stage == "movies":
        success = orchestrator.run_movies(
            strategy=args.movie_strategy,
            target_count=args.movies_count,
            min_vote_count=args.movies_min_votes
        )
    elif args.stage == "series":
        success = orchestrator.run_series(
            strategy="discover",
            target_count=args.series_count,
            load_episodes=args.load_episodes,
            min_vote_count=args.series_min_votes
        )
    elif args.stage == "persons":
        success = orchestrator.run_persons(
            target_count=args.persons_count,
            min_popularity=args.persons_min_popularity,
            load_from_content=True
        )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()