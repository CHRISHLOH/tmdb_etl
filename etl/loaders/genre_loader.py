from base_loader import BaseLoader
from tmdb_client import TMDBClient
from typing import List, Dict
import json
import os


class GenreLoader(BaseLoader):
    """
    Загрузчик жанров из TMDB.
    Поддерживает мультиязычность через translations JSONB.
    """
    
    def __init__(self):
        super().__init__()
        self.client = TMDBClient()
        # Из .env: TARGET_LOCALES=en,ru
        self.target_locales = os.getenv("TARGET_LOCALES", "en,ru").split(",")
    
    def extract(self) -> List[Dict]:
        """
        Получаем жанры для всех целевых языков.
        TMDB возвращает: [{"id": 28, "name": "Action"}, ...]
        """
        genres_by_locale = {}
        
        # Загружаем для каждого языка
        for locale in self.target_locales:
            print(f"  Fetching genres for locale: {locale}")
            genres = self.client.get_genres(language=locale)
            
            for genre in genres:
                genre_id = genre["id"]
                genre_name = genre["name"]
                
                if genre_id not in genres_by_locale:
                    genres_by_locale[genre_id] = {
                        "tmdb_id": genre_id,
                        "machine_name": None,  # заполним ниже
                        "translations": {}
                    }
                
                genres_by_locale[genre_id]["translations"][locale] = genre_name
        
        # Преобразуем в список и добавляем machine_name (на основе английского)
        result = []
        for genre_id, data in genres_by_locale.items():
            # machine_name = английское название в lowercase
            machine_name = data["translations"].get("en", "unknown").lower().replace(" ", "_")
            data["machine_name"] = machine_name
            result.append(data)
        
        return result
    
    def transform(self, raw_data: List[Dict]) -> List[tuple]:
        """
        Трансформация: (genre, translations_json)
        genre = machine_name (action, drama, sci_fi)
        """
        transformed = []
        
        for genre in raw_data:
            machine_name = genre["machine_name"]
            translations_json = json.dumps(genre["translations"], ensure_ascii=False)
            
            transformed.append((machine_name, translations_json))
        
        return transformed
    
    def get_upsert_query(self) -> str:
        return """
            INSERT INTO content_service.genres (genre, translations)
            VALUES (%s, %s::jsonb)
            ON CONFLICT (genre) 
            DO UPDATE SET 
                translations = EXCLUDED.translations
        """


if __name__ == "__main__":
    loader = GenreLoader()
    loader.run()