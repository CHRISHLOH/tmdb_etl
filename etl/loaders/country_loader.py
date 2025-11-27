from base_loader import BaseLoader
from tmdb_client import TMDBClient
from typing import List, Dict
import json
import os


class CountryLoader(BaseLoader):
    """
    Загрузчик стран из TMDB configuration.
    TMDB API возвращает список стран с ISO кодами.
    """
    
    def __init__(self):
        super().__init__()
        self.client = TMDBClient()
        self.target_locales = os.getenv("TARGET_LOCALES", "en,ru").split(",")
    
    def extract(self) -> List[Dict]:
        """
        TMDB Configuration содержит список стран.
        Структура: {iso_3166_1: "US", english_name: "United States of America", native_name: "..."}
        """
        config = self.client.get_configuration()
        
        # В TMDB конфигурации стран нет напрямую, но можно взять из /configuration/countries
        # Или использовать стандартный справочник ISO 3166-1
        
        # Для демо используем запрос к отдельному эндпоинту
        countries_data = self.client._request("/configuration/countries")
        
        if not countries_data:
            print("⚠️  Could not fetch countries from TMDB, using fallback")
            return self._get_fallback_countries()
        
        result = []
        for country in countries_data:
            iso_code = country.get("iso_3166_1")
            english_name = country.get("english_name")
            native_name = country.get("native_name", english_name)
            
            if iso_code and english_name:
                result.append({
                    "iso_code": iso_code,
                    "translations": {
                        "en": english_name,
                        # native_name может быть на любом языке
                        "native": native_name
                    }
                })
        
        return result
    
    def _get_fallback_countries(self) -> List[Dict]:
        """
        Fallback: основные страны если TMDB не отвечает.
        В продакшене лучше использовать полный справочник ISO 3166-1.
        """
        return [
            {"iso_code": "US", "translations": {"en": "United States", "ru": "США"}},
            {"iso_code": "GB", "translations": {"en": "United Kingdom", "ru": "Великобритания"}},
            {"iso_code": "FR", "translations": {"en": "France", "ru": "Франция"}},
            {"iso_code": "DE", "translations": {"en": "Germany", "ru": "Германия"}},
            {"iso_code": "RU", "translations": {"en": "Russia", "ru": "Россия"}},
            {"iso_code": "JP", "translations": {"en": "Japan", "ru": "Япония"}},
            {"iso_code": "KR", "translations": {"en": "South Korea", "ru": "Южная Корея"}},
            {"iso_code": "CN", "translations": {"en": "China", "ru": "Китай"}},
            {"iso_code": "IN", "translations": {"en": "India", "ru": "Индия"}},
            {"iso_code": "CA", "translations": {"en": "Canada", "ru": "Канада"}},
        ]
    
    def transform(self, raw_data: List[Dict]) -> List[tuple]:
        """
        Трансформация: (iso_code, translations_json)
        """
        transformed = []
        
        for country in raw_data:
            iso_code = country["iso_code"]
            translations_json = json.dumps(country["translations"], ensure_ascii=False)
            
            transformed.append((iso_code, translations_json))
        
        return transformed
    
    def get_upsert_query(self) -> str:
        return """
            INSERT INTO content_service.countries (iso_code, translations)
            VALUES (%s, %s::jsonb)
            ON CONFLICT (iso_code) 
            DO UPDATE SET 
                translations = EXCLUDED.translations
        """


if __name__ == "__main__":
    loader = CountryLoader()
    loader.run()