import psycopg
import os
import uuid
from dotenv import load_dotenv
from datetime import date

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
                try:
                    cur.execute("""
                                CREATE TABLE IF NOT EXISTS song (
                                    id UUID PRIMARY KEY,
                                    comment TEXT,
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
                                    id UUID PRIMARY KEY,
                                    name TEXT NOT NULL
                                );
                                """)

                    cur.execute("""
                                CREATE TABLE IF NOT EXISTS album (
                                    id UUID PRIMARY KEY,
                                    title TEXT NOT NULL,
                                    date DATE
                                )
                                """)

                    cur.execute("""
                                CREATE TABLE IF NOT EXISTS song_album (
                                    id UUID PRIMARY KEY,
                                    song_id UUID REFERENCES song(id) ON DELETE CASCADE, 
                                    album_id UUID REFERENCES album(id) ON DELETE CASCADE, 
                                    tracknumber INT
                                )
                                """)

                    cur.execute("""
                                CREATE TABLE IF NOT EXISTS song_artist (
                                    id UUID PRIMARY KEY,
                                    song_id UUID REFERENCES song(id) ON DELETE CASCADE, 
                                    artist_id UUID REFERENCES artist(id) ON DELETE CASCADE
                                )
                                """)

                    cur.execute("""
                                CREATE TABLE IF NOT EXISTS album_artist (
                                    id UUID PRIMARY KEY,
                                    album_id UUID REFERENCES album(id) ON DELETE CASCADE, 
                                    artist_id UUID REFERENCES artist(id) ON DELETE CASCADE
                                )
                                """)

                    conn.commit()
                    print("Creating database tables was succesfull!")
                except Exception as e:
                    print("Creating database tables did not work:", e)

    def drop_table(self, table_name: str) -> None:
        """Drop a table in the PostgreSQL database.
        """
        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        f"""DROP TABLE IF EXISTS {table_name} CASCADE;""")

                    conn.commit()
                except Exception as e:
                    print(f"Dropping the table {table_name} did not work:", e)

    def add_data(self, song_dict: dict) -> None:
        """Add all song data (e.g. album, artist, song and many-to-many relationships) into the PostgreSQL database.

        Args:
            song_dict (dict): Song data, e.g. album, artist, rhythm data, audio features
        """
        self.add_song(song_dict)
        self.add_album(song_dict)
        self.add_artist(song_dict)

    def add_song(self, song_dict: dict) -> None:
        """Add a song to the song table of the PostgreSQL database.

        Args:
            song_dict (dict): Song data, e.g. album, artist, rhythm data, audio features
        """
        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                try:
                    # Select the correct values to insert into database
                    values = [uuid.uuid4()]

                    for key, value in song_dict.items():
                        if key not in ["album", "artist", "date", "tracknumber"]:
                            if key == "valence_arousal":
                                values.append(value[0])
                                values.append(value[1])
                            else:
                                values.append(value)

                    # Add WHERE NOT clauses
                    values.append(song_dict["title"])
                    print(values)

                    # Get the column names from song table
                    cur.execute("SELECT * FROM song LIMIT 0")
                    column_names: list = [description[0]
                                          for description in cur.description]
                    sql_column_names = "(" + ", ".join(column_names) + ")"
                    sql_statement = f""" 
                                        INSERT INTO song {sql_column_names}
                                        SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                        WHERE NOT EXISTS (SELECT * FROM song WHERE title = %s)
                                    """
                    cur.execute(sql_statement, values)
                    print(
                        f'Adding song {song_dict["title"]} to the database successful!')
                except Exception as e:
                    print(
                        f'Adding a song {song_dict["title"]} did not work:', e)

    def add_album(self, song_dict: dict) -> None:
        """Add an album to the album table of the PostgreSQL database.

        Args:
            song_dict (dict): Song data, e.g. album, artist, rhythm data, audio features
        """
        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(""" INSERT INTO album (id, title, date)
                                    SELECT %s, %s, %s
                                    WHERE NOT EXISTS (SELECT * FROM album WHERE title = %s AND date = %s)
                                """,
                                (uuid.uuid4(),
                                 song_dict["album"],
                                 date(int(song_dict["date"]), 1, 1),
                                 song_dict["album"],
                                 date(int(song_dict["date"]), 1, 1)))
                    print(
                        f'Adding the album {song_dict["album"]} to the database was successful!')
                except Exception as e:
                    print(
                        f'Adding an album {song_dict["album"]} did not work:', e)

    def add_artist(self, song_dict: dict) -> None:
        """Add an artist to the artist table of the PostgreSQL database.

        Args:
            song_dict (dict): Song data, e.g. album, artist, rhythm data, audio features
        """
        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(f"""INSERT INTO artist (id, name)
                                    SELECT %s, %s
                                    WHERE NOT EXISTS (SELECT 1 FROM artist WHERE name = %s)
                                """,
                                (uuid.uuid4(),
                                 song_dict["artist"],
                                 song_dict["artist"]))
                    print(
                        f'Adding the artist {song_dict["artist"]} to the database was successful!')
                except Exception as e:
                    print(
                        f'Adding artist {song_dict["artist"]} did not work:', e)

    def create_unique_index(self, table_name: str, columns: list[str]) -> None:
        """Create a unique index for columns in a table. Important for ON CONFLICT sql statements.

        Args:
            table_name (str): Name of the table, e.g. song
            columns (list[str]): A list of columns in the table, e.g. [comment, date, title]
        """
        columns_string = ", ".join(columns)
        name_of_index = f"idx_{table_name}_" + "_".join(columns)
        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        f"CREATE UNIQUE INDEX {name_of_index} ON {table_name} ({columns_string});")
                    print(
                        f'Creating a unique index for "{table_name}" was successfull!')
                except Exception as e:
                    print(
                        f'Creating a unique index for "{table_name}" did not work:', e)

    def delete_all_entries(self, table_name: str) -> None:
        """Delete all entries from a table, e.g. all rows in "song" table.

        Args:
            table_name (str): Name of the table, e.g. "song"
        """
        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(f"DELETE FROM {table_name};")
                except Exception as e:
                    print(f'Could not delete entries in "{table_name}"')


if __name__ == "__main__":
    song_dict = {'album': 'Satch Plays Fats', 'artist': 'Louis Armstrong', 'comment:n': 'Converted by https://spotifydown.com', 'date': '1955', 'title': "I'm Crazy 'Bout My Baby - Edit", 'tracknumber': '', 'valence_arousal': (
        5.886054, 5.5037227), 'danceable_not_danceable': 0.19786096256684493, 'aggressive_non_aggressive': 0.0, 'happy_non_happy': 0.09090909090909091, 'party_non_party': 0.7165775401069518, 'relaxed_non_relaxed': 0.0, 'sad_non_sad': 0.18085106382978725, 'acoustic_non_acoustic': 0.475177304964539, 'electronic_non_electronic': 0.0, 'instrumental_voice': 0.3333333333333333, 'female_male': 0.19858156028368795, 'bright_dark': 0.014184397163120567, 'acoustic_electronic': 0.014184397163120567, 'dry_wet': 0.7553191489361702}
    db_connector = DBConnector()

    # Deleting all entries
    # db_connector.delete_all_entries("song")
    # db_connector.delete_all_entries("album")
    # db_connector.delete_all_entries("artist")

    # Deleting all tables
    # db_connector.drop_table("song")
    # db_connector.drop_table("album")
    # db_connector.drop_table("artist")
    # db_connector.drop_table("song_album")
    # db_connector.drop_table("song_artist")
    # db_connector.drop_table("album_artist")

    # Creating tables
    # db_connector.create_tables()

    # Add data
    db_connector.add_data(song_dict)
