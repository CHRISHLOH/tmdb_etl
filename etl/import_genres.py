import requests
from db import get_connection
import os
from dotenv import load_dotenv

load_dotenv()

TMDB_BEARER = os.getenv("TMDB_BEARER_TOKEN")
URL = "https://api.themoviedb.org/3/genre/movie/list?language=en"

def import_genres():
    print("Starting genre import...")
    print("Using API key:", "FOUND" if TMDB_BEARER else "MISSING")

    headers = {
        "Authorization": f"Bearer {TMDB_BEARER}",
        "accept": "application/json"
    }

    response = requests.get(URL, headers=headers)

    print("TMDB response status:", response.status_code)
    print("TMDB response body:", response.text[:300])

    if response.status_code != 200:
        print("TMDB request failed, aborting.")
        return

    data = response.json()
    genres = data.get("genres", [])

    print(f"Genres received: {len(genres)}")

    if not genres:
        print("No genres found in response.")
        return

    conn = get_connection()
    cur = conn.cursor()

    inserted = 0

    for genre in genres:
        name = genre["name"]

        cur.execute("""
            INSERT INTO content_service.genres (genre, translations)
            VALUES (%s, %s::jsonb)
            ON CONFLICT (genre) DO NOTHING
        """, (name, '{}'))

        inserted += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"Genres inserted: {inserted}")

if __name__ == "__main__":
    import_genres()
