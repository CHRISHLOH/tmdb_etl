from base_loader import BaseLoader
from tmdb_client import TMDBClient
from typing import List, Dict
import json
import os

class LanguageLoader(BaseLoader):
    """
    Загрузчик языков из TMDB configuration.
    """

    def __init__(self):
        super().__init__()
        self.client = TMDBClient()
        self.target_locales = os.getenv("TARGET_LOCALES", "en,ru").split(",")

    def extract(self) -> List[Dict]:
        """
        TMDB Configuration содержит список языков.
        Структура: {iso_639_1: "en", english_name: "English", name: "English"}
        """
        # TMDB API endpoint для списка языков
        languages = self.client._request("/configuration/languages")
        
        if not languages:
            print("⚠️  Could not fetch languages from TMDB API")
            return []

        result = []
        for lang in languages:
            iso_code = lang.get("iso_639_1")
            english_name = lang.get("english_name")
            native_name = lang.get("name", english_name)

            if iso_code and english_name:
                result.append({
                    "iso_code": iso_code,
                    "native_name": native_name,
                    "translations": {
                        "en": english_name
                        # TODO: Можно добавить ru переводы вручную для основных языков
                    }
                })

        return result

    def transform(self, raw_data: List[Dict]) -> List[tuple]:
        """
        Трансформация: (iso_code, native_name, translations_json)
        """
        transformed = []
        for lang in raw_data:
            iso_code = lang["iso_code"]
            native_name = lang["native_name"]
            translations_json = json.dumps(lang["translations"], ensure_ascii=False)
            transformed.append((iso_code, native_name, translations_json))

        return transformed

    def get_upsert_query(self) -> str:
        return """
            INSERT INTO content_service.languages (iso_code, native_name, translations)
            VALUES (%s, %s, %s::jsonb)
            ON CONFLICT (iso_code) 
            DO UPDATE SET 
                native_name = EXCLUDED.native_name,
                translations = EXCLUDED.translations
        """


if __name__ == "__main__":
    loader = LanguageLoader()
    loader.run()