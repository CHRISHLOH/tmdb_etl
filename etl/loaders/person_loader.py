"""
PersonLoader - загрузка персон из TMDB с полными связями

Что загружается:
1. persons - базовая информация (имя, дата рождения, фото)
2. person_translations - переводы (имя, биография)
3. person_careers - связь персон с карьерами (актёр, режиссёр и т.д.)
4. person_countries - связь персон со странами
5. content_persons - связь персон с фильмами/сериалами (автоматически из credits)

Стратегия:
- Собираем персон из credits уже загруженных фильмов/сериалов
- Для топ-N персон загружаем полные данные через /person/{id}
"""

import asyncio
import aiohttp
from typing import List, Dict, Set, Tuple
from base_loader import BaseLoader
from tmdb_client import AsyncTMDBClient
from psycopg2.extras import execute_batch
from tqdm import tqdm
import os


class PersonLoader(BaseLoader):
    """
    Загрузчик персон из TMDB
    """
    
    # Маппинг TMDB department -> career machine_name
    CAREER_MAPPING = {
        "Acting": "actor",
        "Directing": "director",
        "Writing": "writer",
        "Production": "producer",
        "Camera": "cinematographer",
        "Editing": "editor",
        "Sound": "composer",
        "Art": "production_designer",
        "Costume & Make-Up": "costume_designer",
        "Visual Effects": "vfx_artist",
        "Crew": "crew"
    }
    
    # Маппинг TMDB job -> career (для более точного определения)
    JOB_TO_CAREER = {
        "Director": "director",
        "Writer": "writer",
        "Screenplay": "writer",
        "Producer": "producer",
        "Executive Producer": "producer",
        "Director of Photography": "cinematographer",
        "Editor": "editor",
        "Original Music Composer": "composer",
        "Production Design": "production_designer",
        "Costume Design": "costume_designer"
    }
    
    def __init__(
        self,
        target_count: int = 5000,
        min_popularity: float = 5.0,
        load_from_content: bool = True
    ):
        """
        Args:
            target_count: Количество персон для загрузки
            min_popularity: Минимальная популярность (фильтр качества)
            load_from_content: Собирать персон из уже загруженного контента
        """
        super().__init__()
        
        self.target_count = target_count
        self.min_popularity = min_popularity
        self.load_from_content = load_from_content
        
        self.client = AsyncTMDBClient()
        self.target_locales = os.getenv("TARGET_LOCALES", "en,ru").split(",")
        
        # Справочники (заполним в __enter__)
        self.career_map = {}  # {career_name: id}
        self.country_map = {}  # {iso_code: id}
        
        # Для хранения content_persons (заполним при загрузке)
        self.content_persons_data = []
    
    def __enter__(self):
        """Загружаем справочники и connection"""
        super().__enter__()
        self._load_reference_data()
        return self
    
    def _load_reference_data(self):
        """Загрузка справочников careers и countries"""
        print("  Loading reference data...")
        
        # Careers
        self.cursor.execute("SELECT id, career FROM content_service.careers")
        self.career_map = {row[1]: row[0] for row in self.cursor.fetchall()}
        
        # Countries
        self.cursor.execute("SELECT id, iso_code FROM content_service.countries")
        self.country_map = {row[1]: row[0] for row in self.cursor.fetchall()}
        
        print(f"  ✓ {len(self.career_map)} careers, {len(self.country_map)} countries")
        
        if len(self.career_map) == 0:
            print("  ⚠️  WARNING: No careers found! Run career_loader first")
        
        print()
    
    def extract(self) -> List[Dict]:
        """
        Извлечение данных персон:
        1. Собираем ID персон из credits контента
        2. Загружаем полные данные для топ-N по популярности
        """
        
        if self.load_from_content:
            # Стратегия 1: собрать персон из уже загруженного контента
            person_ids = self._extract_persons_from_content()
        else:
            # Стратегия 2: загрузить топ персон напрямую из TMDB
            person_ids = asyncio.run(self._extract_top_persons())
        
        if not person_ids:
            print("⚠️  No person IDs found")
            return []
        
        # Загружаем полные данные персон
        print(f"\n📥 Fetching full data for {len(person_ids)} persons...")
        persons_data = asyncio.run(
            self.client.fetch_persons_batch(person_ids)
        )
        
        print(f"✅ Fetched {len(persons_data)} persons\n")
        return persons_data
    
    def _extract_persons_from_content(self) -> List[int]:
        """
        Собрать ID персон из credits уже загруженного контента.
        
        Логика:
        1. Для каждого фильма/сериала есть credits (cast + crew)
        2. Собираем уникальные person_id
        3. Подсчитываем popularity (сколько раз появляется)
        4. Берём топ-N
        """
        print("\n📥 Collecting persons from content credits...")
        
        # Получаем все content_id
        self.cursor.execute("""
            SELECT id FROM content_service.content 
            WHERE content_type IN ('movie', 'series')
            LIMIT 10000
        """)
        content_ids = [row[0] for row in self.cursor.fetchall()]
        
        if not content_ids:
            print("⚠️  No content found in DB")
            return []
        
        print(f"  Found {len(content_ids)} content items")
        print(f"  Fetching credits from TMDB...")
        
        # Собираем credits для всех content
        person_stats = {}  # {person_id: {popularity, departments, content_ids}}
        
        async def fetch_all_credits():
            async with aiohttp.ClientSession(headers=self.client.headers) as session:
                tasks = []
                for content_id in content_ids[:1000]:  # Ограничим для скорости
                    tasks.append(self._fetch_content_credits(session, content_id))
                
                results = []
                batch_size = 100
                for i in range(0, len(tasks), batch_size):
                    batch = tasks[i:i + batch_size]
                    batch_results = await asyncio.gather(*batch)
                    results.extend(batch_results)
                    print(f"    Progress: {i + len(batch)}/{len(tasks)}")
                
                return results
        
        credits_list = asyncio.run(fetch_all_credits())
        
        # Обрабатываем credits
        for content_id, credits in zip(content_ids[:1000], credits_list):
            if not credits:
                continue
            
            # Cast (актёры)
            for person in credits.get("cast", []):
                person_id = person["id"]
                if person_id not in person_stats:
                    person_stats[person_id] = {
                        "popularity": person.get("popularity", 0),
                        "departments": set(),
                        "content_ids": set()
                    }
                person_stats[person_id]["departments"].add("Acting")
                person_stats[person_id]["content_ids"].add(content_id)
            
            # Crew (остальные)
            for person in credits.get("crew", []):
                person_id = person["id"]
                department = person.get("department", "Crew")
                
                if person_id not in person_stats:
                    person_stats[person_id] = {
                        "popularity": person.get("popularity", 0),
                        "departments": set(),
                        "content_ids": set()
                    }
                person_stats[person_id]["departments"].add(department)
                person_stats[person_id]["content_ids"].add(content_id)
        
        # Сортируем по количеству появлений × popularity
        person_scores = []
        for person_id, stats in person_stats.items():
            score = len(stats["content_ids"]) * stats["popularity"]
            person_scores.append((person_id, score))
        
        person_scores.sort(key=lambda x: x[1], reverse=True)
        
        # Берём топ-N
        top_persons = [pid for pid, _ in person_scores[:self.target_count]]
        
        print(f"✅ Collected {len(top_persons)} top persons from content\n")
        return top_persons
    
    async def _fetch_content_credits(
        self, 
        session: aiohttp.ClientSession, 
        content_id: int
    ) -> Dict:
        """Получить credits для контента"""
        return await self.client._request(
            session,
            f"/movie/{content_id}/credits",
            params={"language": "en"}
        )
    
    async def _extract_top_persons(self) -> List[int]:
        """
        Альтернативная стратегия: загрузить топ персон через /person/popular
        """
        print("\n📥 Fetching top persons from TMDB /person/popular...")
        
        person_ids = []
        pages_needed = (self.target_count + 19) // 20
        
        async with aiohttp.ClientSession(headers=self.client.headers) as session:
            for page in range(1, min(pages_needed + 1, 501)):
                data = await self.client._request(
                    session,
                    "/person/popular",
                    params={"page": page}
                )
                
                if data and "results" in data:
                    for person in data["results"]:
                        if person.get("popularity", 0) >= self.min_popularity:
                            person_ids.append(person["id"])
                
                if len(person_ids) >= self.target_count:
                    break
        
        print(f"✅ Collected {len(person_ids)} person IDs\n")
        return person_ids[:self.target_count]
    
    def transform(self, raw_data: List[Dict]) -> Dict[str, List[Tuple]]:
        """
        Трансформация TMDB данных в формат БД
        
        Returns:
            {
                "persons": [(id, name, lastname, birth_date, ...)],
                "person_translations": [(person_id, locale, name, bio)],
                "person_careers": [(person_id, career_id, order)],
                "person_countries": [(person_id, country_id)]
            }
        """
        persons_data = []
        translations_data = []
        careers_data = []
        countries_data = []
        
        for person in raw_data:
            person_id = person["id"]
            
            # 1. persons
            name_parts = person.get("name", "Unknown").split(" ", 1)
            first_name = name_parts[0]
            last_name = name_parts[1] if len(name_parts) > 1 else None
            
            birth_date = person.get("birthday")
            death_date = person.get("deathday")
            
            # Gender: 0=not set, 1=female, 2=male, 3=non-binary
            gender_map = {0: None, 1: "female", 2: "male", 3: "other"}
            gender = gender_map.get(person.get("gender"), None)
            
            # Место рождения -> country
            place_of_birth = person.get("place_of_birth", "")
            country_id = self._extract_country_from_place(place_of_birth)
            
            photo_url = person.get("profile_path")
            
            persons_data.append((
                person_id,
                first_name,
                last_name,
                birth_date,
                death_date,
                gender,
                country_id,
                None,  # city_id (пока None)
                photo_url
            ))
            
            # 2. person_translations
            for locale in self.target_locales:
                # Для английского используем оригинальное имя
                if locale == "en":
                    name = person.get("name", "Unknown")
                    bio = person.get("biography", "")
                else:
                    # Для других языков нужны переводы (пока используем английское)
                    name = person.get("name", "Unknown")
                    bio = ""
                
                if name:
                    name_parts = name.split(" ", 1)
                    translations_data.append((
                        person_id,
                        locale,
                        name_parts[0],
                        name_parts[1] if len(name_parts) > 1 else None,
                        bio if locale == "en" else None
                    ))
            
            # 3. person_careers (из known_for_department)
            department = person.get("known_for_department", "Acting")
            career_name = self.CAREER_MAPPING.get(department, "actor")
            
            if career_name in self.career_map:
                careers_data.append((
                    person_id,
                    self.career_map[career_name],
                    0  # display_order
                ))
            
            # 4. person_countries
            if country_id:
                countries_data.append((person_id, country_id))
        
        print(f"✅ Transformed {len(raw_data)} persons")
        print(f"   - Translations: {len(translations_data)}")
        print(f"   - Careers: {len(careers_data)}")
        print(f"   - Countries: {len(countries_data)}")
        
        return {
            "persons": persons_data,
            "person_translations": translations_data,
            "person_careers": careers_data,
            "person_countries": countries_data
        }
    
    def _extract_country_from_place(self, place_of_birth: str) -> int:
        """
        Извлечь страну из места рождения.
        Формат: "New York City, New York, USA"
        """
        if not place_of_birth:
            return None
        
        # Ищем ISO код в конце строки
        parts = place_of_birth.split(",")
        if len(parts) > 0:
            country_part = parts[-1].strip()
            
            # Маппинг стран (упрощённый)
            country_mapping = {
                "USA": "US",
                "United States": "US",
                "UK": "GB",
                "United Kingdom": "GB",
                "England": "GB",
                "Russia": "RU",
                "USSR": "RU",
                "France": "FR",
                "Germany": "DE",
                "Italy": "IT",
                "Spain": "ES",
                "Canada": "CA",
                "Australia": "AU",
                "Japan": "JP",
                "China": "CN",
                "India": "IN",
                "South Korea": "KR",
                "Mexico": "MX",
                "Brazil": "BR"
            }
            
            iso_code = country_mapping.get(country_part, country_part)
            
            if iso_code in self.country_map:
                return self.country_map[iso_code]
        
        return None
    
    def get_upsert_query(self) -> str:
        """Не используется"""
        pass
    
    def run(self):
        """Запуск загрузки персон"""
        print(f"\n{'='*60}")
        print(f"Person Loader")
        print(f"Target: {self.target_count} persons")
        print(f"Min popularity: {self.min_popularity}")
        print(f"Load from content: {self.load_from_content}")
        print(f"{'='*60}\n")
        
        with self:
            # Extract
            raw_data = self.extract()
            if not raw_data:
                print("⚠️  No data extracted")
                return
            
            # Transform
            print("\n⚙️  Transforming data...")
            transformed = self.transform(raw_data)
            
            # Load
            self._load_all_tables(transformed)
        
        print(f"\n✅ Person Loader completed successfully\n")
    
    def _load_all_tables(self, data: Dict[str, List]):
        """Загрузка всех таблиц"""
        
        # 1. persons
        if data["persons"]:
            print(f"\n📤 Loading persons ({len(data['persons'])} records)...")
            query = """
                INSERT INTO content_service.persons 
                (id, original_name, original_lastname, birth_date, death_date, 
                 gender, country_id, city_id, photo_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    original_name = EXCLUDED.original_name,
                    original_lastname = EXCLUDED.original_lastname,
                    birth_date = EXCLUDED.birth_date,
                    death_date = EXCLUDED.death_date,
                    gender = EXCLUDED.gender,
                    country_id = EXCLUDED.country_id,
                    photo_url = EXCLUDED.photo_url,
                    updated_at = NOW()
            """
            self._batch_insert(query, data["persons"], "persons")
        
        # 2. person_translations
        if data["person_translations"]:
            print(f"\n📤 Loading person_translations ({len(data['person_translations'])} records)...")
            query = """
                INSERT INTO content_service.person_translations 
                (person_id, locale, locale_name, locale_lastname, biography)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (person_id, locale) DO UPDATE SET
                    locale_name = EXCLUDED.locale_name,
                    locale_lastname = EXCLUDED.locale_lastname,
                    biography = EXCLUDED.biography,
                    updated_at = NOW()
            """
            self._batch_insert(query, data["person_translations"], "person_translations")
        
        # 3. person_careers
        if data["person_careers"]:
            print(f"\n📤 Loading person_careers ({len(data['person_careers'])} records)...")
            query = """
                INSERT INTO content_service.person_careers 
                (person_id, career_id, display_order)
                VALUES (%s, %s, %s)
                ON CONFLICT (person_id, career_id) DO UPDATE SET
                    display_order = EXCLUDED.display_order
            """
            self._batch_insert(query, data["person_careers"], "person_careers")
        
        # 4. person_countries
        if data["person_countries"]:
            print(f"\n📤 Loading person_countries ({len(data['person_countries'])} records)...")
            query = """
                INSERT INTO content_service.person_countries 
                (person_id, country_id)
                VALUES (%s, %s)
                ON CONFLICT (person_id, country_id) DO NOTHING
            """
            self._batch_insert(query, data["person_countries"], "person_countries")
    
    def _batch_insert(self, query: str, data: List, name: str):
        """Batch insert с progress bar"""
        with tqdm(total=len(data), desc=f"Loading {name}") as pbar:
            for i in range(0, len(data), self.batch_size):
                batch = data[i:i + self.batch_size]
                execute_batch(self.cursor, query, batch, page_size=self.batch_size)
                pbar.update(len(batch))
        
        print(f"  ✓ Loaded {len(data)} {name}")


if __name__ == "__main__":
    # Загрузить топ 5000 персон из уже загруженного контента
    loader = PersonLoader(
        target_count=5000,
        min_popularity=5.0,
        load_from_content=True
    )
    loader.run()