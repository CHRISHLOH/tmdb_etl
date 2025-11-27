"""
ETL Orchestrator
Запускает все загрузчики в правильном порядке с учетом зависимостей.

Граф зависимостей:
1. Справочники (независимые): genres, countries, languages, careers
2. Контент (зависит от справочников): movies, series
3. Персоны (зависит от countries): persons
4. Связи (зависит от всего): content_persons, awards и т.д.

Usage:
    python run_etl.py --stage dictionaries
    python run_etl.py --stage movies --target-count 1000
    python run_etl.py --stage series --target-count 500
    python run_etl.py --stage all
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
    from loaders.id_export_loader import MovieDetailsLoader
    from loaders.series_loader import SeriesLoader
    # from loaders.person_loader import PersonLoader  # TODO
    # from loaders.career_loader import CareerLoader  # TODO
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
        """Этап 1: Загрузка справочников (параллельно могут грузиться)"""
        print("\n" + "="*70)
        print("STAGE 1: DICTIONARIES")
        print("="*70)
        
        stages = [
            ("Genres", lambda: GenreLoader().run()),
            ("Countries", lambda: CountryLoader().run()),
            ("Languages", lambda: LanguageLoader().run()),
            # ("Careers", lambda: CareerLoader().run()),  # TODO: добавить когда будет готов
        ]
        
        success_count = 0
        for name, loader_func in stages:
            if self.run_stage(name, loader_func):
                success_count += 1
        
        print(f"\n📊 Dictionaries stage: {success_count}/{len(stages)} successful")
        return success_count == len(stages)
    
    def run_movies(self, target_count: int = 1000, min_popularity: float = 20):
        """Этап 2: Загрузка фильмов через daily exports + API"""
        print("\n" + "="*70)
        print(f"STAGE 2: MOVIES (target: {target_count}, min popularity: {min_popularity})")
        print("="*70)
        
        return self.run_stage(
            "Movies", 
            lambda: MovieDetailsLoader(
                target_count=target_count,
                min_popularity=min_popularity
            ).run()
        )
    
    def run_series(
        self,
        strategy: str = "discover",
        target_count: int = 500,
        load_episodes: bool = False,
        **strategy_kwargs
    ):
        """
        Этап 3: Загрузка сериалов.
        
        Args:
            strategy: "discover"
            target_count: Количество сериалов
            load_episodes: Загружать ли эпизоды (МЕДЛЕННО, для MVP = False)
            **strategy_kwargs: Параметры для стратегии
        """
        print("\n" + "="*70)
        print(f"STAGE 3: SERIES")
        print(f"Strategy: {strategy}")
        print(f"Target: {target_count} series")
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
                **strategy_kwargs
            ).run()
        )
    
    def run_persons(self, max_persons: int = 1000):
        """Этап 4: Загрузка персон"""
        print("\n" + "="*70)
        print(f"STAGE 4: PERSONS (max {max_persons} persons)")
        print("="*70)
        
        # TODO: Implement PersonLoader
        print("⚠️  PersonLoader not implemented yet")
        return True
    
    def run_all(self, target_count: int = 1000, min_popularity: float = 20):
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
        if not self.run_movies(target_count=target_count, min_popularity=min_popularity):
            print("\n⚠️  Movies stage failed, but continuing...")
        
        # Этап 3: Сериалы (опционально)
        # if not self.run_series(target_count=500, load_episodes=False):
        #     print("\n⚠️  Series stage failed, but continuing...")
        
        # Этап 4: Персоны
        # if not self.run_persons():
        #     print("\n⚠️  Persons stage failed, but continuing...")
        
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
    parser = argparse.ArgumentParser(description="TMDB ETL Orchestrator")
    parser.add_argument(
        "--stage",
        choices=["all", "dictionaries", "movies", "series", "persons"],
        default="all",
        help="Which stage to run"
    )
    parser.add_argument(
        "--target-count",
        type=int,
        default=1000,
        help="Target number of items to load (movies or series)"
    )
    parser.add_argument(
        "--min-popularity",
        type=float,
        default=20.0,
        help="Minimum popularity threshold for movies"
    )
    parser.add_argument(
        "--load-episodes",
        action="store_true",
        help="Load episodes for series (SLOW, not recommended for MVP)"
    )
    
    args = parser.parse_args()
    
    orchestrator = ETLOrchestrator()
    
    if args.stage == "all":
        success = orchestrator.run_all(
            target_count=args.target_count,
            min_popularity=args.min_popularity
        )
    elif args.stage == "dictionaries":
        success = orchestrator.run_dictionaries()
    elif args.stage == "movies":
        success = orchestrator.run_movies(
            target_count=args.target_count,
            min_popularity=args.min_popularity
        )
    elif args.stage == "series":
        success = orchestrator.run_series(
            strategy="discover",
            target_count=args.target_count,
            load_episodes=args.load_episodes,
            min_vote_count=200
        )
    elif args.stage == "persons":
        success = orchestrator.run_persons(max_persons=1000)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()