"""
CareerLoader - загрузка профессий (careers)

Профессии для киноиндустрии с переводами.
"""

from base_loader import BaseLoader
from typing import List, Dict
import json
import os


class CareerLoader(BaseLoader):
    """
    Загрузчик профессий (карьер) для киноиндустрии.
    Статичный справочник с переводами.
    """
    
    def __init__(self):
        super().__init__()
        self.target_locales = os.getenv("TARGET_LOCALES", "en,ru").split(",")
    
    def extract(self) -> List[Dict]:
        """
        Статичный список профессий с переводами.
        """
        careers = [
            {
                "career": "actor",
                "translations": {
                    "en": "Actor",
                    "ru": "Актёр"
                }
            },
            {
                "career": "director",
                "translations": {
                    "en": "Director",
                    "ru": "Режиссёр"
                }
            },
            {
                "career": "writer",
                "translations": {
                    "en": "Writer",
                    "ru": "Сценарист"
                }
            },
            {
                "career": "producer",
                "translations": {
                    "en": "Producer",
                    "ru": "Продюсер"
                }
            },
            {
                "career": "cinematographer",
                "translations": {
                    "en": "Cinematographer",
                    "ru": "Оператор"
                }
            },
            {
                "career": "editor",
                "translations": {
                    "en": "Editor",
                    "ru": "Монтажёр"
                }
            },
            {
                "career": "composer",
                "translations": {
                    "en": "Composer",
                    "ru": "Композитор"
                }
            },
            {
                "career": "production_designer",
                "translations": {
                    "en": "Production Designer",
                    "ru": "Художник-постановщик"
                }
            },
            {
                "career": "costume_designer",
                "translations": {
                    "en": "Costume Designer",
                    "ru": "Художник по костюмам"
                }
            },
            {
                "career": "vfx_artist",
                "translations": {
                    "en": "VFX Artist",
                    "ru": "Художник по спецэффектам"
                }
            },
            {
                "career": "sound_designer",
                "translations": {
                    "en": "Sound Designer",
                    "ru": "Звукорежиссёр"
                }
            },
            {
                "career": "casting_director",
                "translations": {
                    "en": "Casting Director",
                    "ru": "Режиссёр по кастингу"
                }
            },
            {
                "career": "stunt_coordinator",
                "translations": {
                    "en": "Stunt Coordinator",
                    "ru": "Координатор трюков"
                }
            },
            {
                "career": "crew",
                "translations": {
                    "en": "Crew",
                    "ru": "Съёмочная группа"
                }
            }
        ]
        
        return careers
    
    def transform(self, raw_data: List[Dict]) -> List[tuple]:
        """
        Трансформация: (career, translations_json)
        """
        transformed = []
        
        for career in raw_data:
            career_name = career["career"]
            translations_json = json.dumps(career["translations"], ensure_ascii=False)
            
            transformed.append((career_name, translations_json))
        
        return transformed
    
    def get_upsert_query(self) -> str:
        return """
            INSERT INTO content_service.careers (career, translations)
            VALUES (%s, %s::jsonb)
            ON CONFLICT (career) 
            DO UPDATE SET 
                translations = EXCLUDED.translations
        """


if __name__ == "__main__":
    loader = CareerLoader()
    loader.run()