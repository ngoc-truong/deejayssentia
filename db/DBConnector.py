import psycopg
import os
from dotenv import load_dotenv


load_dotenv()


class DBConnector:
    """A class to interact with a PostgreSQL database (e.g. create and drop tables).
    """

    def __init__(self):
        self.__DB_NAME: str = os.getenv("DB_NAME")
        self.__DB_USER: str = os.getenv("DB_USER")
        self.__DB_PASSWORD: str = os.getenv("DB_PASSWORD")
        self.__DB_HOST: str = os.getenv("DB_HOST")
        self.__DB_PORT: str = os.getenv("DB_PORT")

    def create_tables(self) -> None:
        """Create tables for song, artist and album.
        """
        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS song (
                        id SERIAL PRIMARY KEY,
                        comment TEXT,
                        date DATE,
                        title TEXT NOT NULL,
                        valence FLOAT,
                        arousal FLOAT,
                        danceable_not_danceable FLOAT,
                        aggressive_non_aggressive FLOAT,
                        happy_non_happy FLOAT,
                        party_non_party FLOAT,
                        relaxed_non_relaxed FLOAT,
                        sad_non_sad FLOAT,
                        acoustic_non_acoustic FLOAT,
                        electronic_non_electronic FLOAT,
                        instrumental_voice FLOAT,
                        female_male FLOAT,
                        bright_dark FLOAT,
                        acoustic_electronic FLOAT,
                        dry_wet FLOAT
                    );
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS artist (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL
                    );
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS album (
                        id SERIAL PRIMARY KEY, 
                        title TEXT NOT NULL
                    )
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS song_album (
                        id SERIAL PRIMARY KEY, 
                        song_id INT REFERENCES song(id) ON DELETE CASCADE, 
                        album_id INT REFERENCES album(id) ON DELETE CASCADE, 
                        tracknumber INT
                    )
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS song_artist (
                        id SERIAL PRIMARY KEY, 
                        song_id INT REFERENCES song(id) ON DELETE CASCADE, 
                        artist_id INT REFERENCES artist(id) ON DELETE CASCADE
                    )
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS album_artist (
                        id SERIAL PRIMARY KEY, 
                        album_id INT REFERENCES album(id) ON DELETE CASCADE, 
                        artist_id INT REFERENCES artist(id) ON DELETE CASCADE
                    )
                """)

                conn.commit()

    def drop_table(self, table_name: str) -> None:
        """Drop a table in the PostgreSQL database.
        """
        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    DROP TABLE IF EXISTS {table_name} CASCADE;
                """)

                conn.commit()
