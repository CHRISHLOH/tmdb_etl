from abc import ABC, abstractmethod
from typing import List, Dict, Any
from psycopg2.extras import execute_batch
from tqdm import tqdm
import os

from db import get_connection


class BaseLoader(ABC):
    """
    Базовый класс для всех ETL загрузчиков.
    
    Паттерн Template Method:
    1. extract() - извлечение данных из источника
    2. transform() - преобразование в формат БД
    3. load() - загрузка в PostgreSQL
    """
    
    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
        self.conn = None
        self.cursor = None
    
    def __enter__(self):
        """Context manager для автоматического управления транзакцией"""
        self.conn = get_connection()
        self.cursor = self.conn.cursor()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Commit или rollback в зависимости от ошибок"""
        if exc_type is None:
            self.conn.commit()
            print("✅ Transaction committed")
        else:
            self.conn.rollback()
            print(f"❌ Transaction rolled back: {exc_val}")
        
        self.cursor.close()
        self.conn.close()
    
    @abstractmethod
    def extract(self) -> List[Dict[str, Any]]:
        """
        Извлечение данных из источника (TMDB API, файлы и т.д.)
        
        Returns:
            List[Dict]: Сырые данные
        """
        pass
    
    @abstractmethod
    def transform(self, raw_data: List[Dict]) -> List[tuple]:
        """
        Преобразование данных в формат для PostgreSQL.
        
        Args:
            raw_data: Сырые данные из extract()
        
        Returns:
            List[tuple]: Данные готовые для INSERT
        """
        pass
    
    @abstractmethod
    def get_upsert_query(self) -> str:
        """
        SQL запрос для INSERT ... ON CONFLICT (upsert).
        
        Returns:
            str: SQL query с плейсхолдерами (%s)
        """
        pass
    
    def load(self, data: List[tuple]):
        """
        Загрузка данных в БД батчами.
        
        Args:
            data: Подготовленные данные (tuple list)
        """
        if not data:
            print("⚠️  No data to load")
            return
        
        query = self.get_upsert_query()
        total = len(data)
        
        print(f"📤 Loading {total} records...")
        
        with tqdm(total=total, desc="Loading to DB") as pbar:
            for i in range(0, total, self.batch_size):
                batch = data[i:i + self.batch_size]
                execute_batch(self.cursor, query, batch, page_size=self.batch_size)
                pbar.update(len(batch))
        
        print(f"✅ Loaded {total} records")
    
    def run(self):
        """
        Основной метод: extract → transform → load.
        Использует context manager для транзакций.
        """
        print(f"\n{'='*60}")
        print(f"Starting {self.__class__.__name__}")
        print(f"{'='*60}\n")
        
        with self:
            # ETL pipeline
            print("📥 Extracting data...")
            raw_data = self.extract()
            
            if not raw_data:
                print("⚠️  No data extracted")
                return
            
            print(f"✅ Extracted {len(raw_data)} records\n")
            
            print("⚙️  Transforming data...")
            transformed = self.transform(raw_data)
            print(f"✅ Transformed {len(transformed)} records\n")
            
            self.load(transformed)
        
        print(f"\n✅ {self.__class__.__name__} completed successfully\n")