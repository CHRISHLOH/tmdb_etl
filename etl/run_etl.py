"""
ETL Orchestrator с поддержкой асинхронной загрузки
Запускает все загрузчики в правильном порядке с учетом зависимостей.

Usage:
    python run_etl.py --stage dictionaries
    python run_etl.py --stage movies --target-count 1000 --async
    python run_etl.py --stage all --async
"""

import argparse
import sys
import time
import asyncio
from datetime import datetime

# Импорты загрузчиков
try:
    from loaders.genre_loader import GenreLoader
    from loaders.country_loader import CountryLoader
    from loaders.language_loader import LanguageLoader
    from loaders.id_export_loader import MovieDetailsLoader
    from tmdb_client import AsyncMovieDetailsLoader  # Новый импорт
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
        target_count: int = 1000, 
        min_popularity: float = 20,
        use_async: bool = False
    ):
        """
        Этап 2: Загрузка фильмов
        
        Args:
            target_count: Количество фильмов
            min_popularity: Минимальная популярность
            use_async: Использовать асинхронную загрузку (в 7-10 раз быстрее!)
        """
        print("\n" + "="*70)
        mode = "ASYNC" if use_async else "SYNC"
        print(f"STAGE 2: MOVIES [{mode}] (target: {target_count}, min popularity: {min_popularity})")
        
        if use_async:
            print("⚡ Using async loader (18 parallel connections, ~45 req/s)")
        else:
            print("🐌 Using sync loader (~9 req/s)")
        
        print("="*70)
        
        if use_async:
            # Асинхронный загрузчик
            loader = AsyncMovieDetailsLoader(
                target_count=target_count,
                min_popularity=min_popularity
            )
            return self.run_stage("Movies (Async)", lambda: loader.run())
        else:
            # Синхронный загрузчик (старый)
            return self.run_stage(
                "Movies (Sync)", 
                lambda: MovieDetailsLoader(
                    target_count=target_count,
                    min_popularity=min_popularity
                ).run()
            )
    
    def run_persons(self, max_persons: int = 1000):
        """Этап 3: Загрузка персон"""
        print("\n" + "="*70)
        print(f"STAGE 3: PERSONS (max {max_persons} persons)")
        print("="*70)
        
        print("⚠️  PersonLoader not implemented yet")
        return True
    
    def run_all(
        self, 
        target_count: int = 1000, 
        min_popularity: float = 20,
        use_async: bool = False
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
            target_count=target_count, 
            min_popularity=min_popularity,
            use_async=use_async
        ):
            print("\n⚠️  Movies stage failed, but continuing...")
        
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
        choices=["all", "dictionaries", "movies", "persons"],
        default="all",
        help="Which stage to run"
    )
    parser.add_argument(
        "--target-count",
        type=int,
        default=1000,
        help="Target number of movies to load (top N by popularity)"
    )
    parser.add_argument(
        "--min-popularity",
        type=float,
        default=20.0,
        help="Minimum popularity threshold for movies"
    )
    parser.add_argument(
        "--async",
        dest="use_async",
        action="store_true",
        help="Use async loader for movies (7-10x faster!)"
    )
    
    args = parser.parse_args()
    
    orchestrator = ETLOrchestrator()
    
    if args.stage == "all":
        success = orchestrator.run_all(
            target_count=args.target_count,
            min_popularity=args.min_popularity,
            use_async=args.use_async
        )
    elif args.stage == "dictionaries":
        success = orchestrator.run_dictionaries()
    elif args.stage == "movies":
        success = orchestrator.run_movies(
            target_count=args.target_count,
            min_popularity=args.min_popularity,
            use_async=args.use_async
        )
    elif args.stage == "persons":
        success = orchestrator.run_persons(max_persons=1000)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()