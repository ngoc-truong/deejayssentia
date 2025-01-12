import psycopg
import os
import uuid
from dotenv import load_dotenv
from datetime import date

load_dotenv()


class DBConnector:
    """A class to interact with a PostgreSQL database (e.g. create, drop tables, insert into etc.).
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
                                    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
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
                                    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                                    name TEXT NOT NULL
                                );
                                """)

                    cur.execute("""
                                CREATE TABLE IF NOT EXISTS album (
                                    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                                    title TEXT NOT NULL,
                                    date DATE
                                );
                                """)

                    cur.execute("""
                                CREATE TABLE IF NOT EXISTS song_album (
                                    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                                    song_id UUID REFERENCES song(id) ON DELETE CASCADE, 
                                    album_id UUID REFERENCES album(id) ON DELETE CASCADE, 
                                    tracknumber INT
                                );
                                """)

                    cur.execute("""
                                CREATE TABLE IF NOT EXISTS song_artist (
                                    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                                    song_id UUID REFERENCES song(id) ON DELETE CASCADE, 
                                    artist_id UUID REFERENCES artist(id) ON DELETE CASCADE
                                );
                                """)

                    cur.execute("""
                                CREATE TABLE IF NOT EXISTS album_artist (
                                    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                                    album_id UUID REFERENCES album(id) ON DELETE CASCADE, 
                                    artist_id UUID REFERENCES artist(id) ON DELETE CASCADE
                                );
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
        """Add all song data (e.g. album, artist, song and many-to-many relationships) into the PostgreSQL tables, e.g. song, album, artist.

        Args:
            song_dict (dict): Song data, e.g. album, artist, rhythm data, audio features
        """
        song_id: uuid.UUID = self.add_song(song_dict)
        album_id: uuid.UUID = self.add_album(song_dict)
        artist_id: uuid.UUID = self.add_artist(song_dict)

        self.add_relation(
            "song_album", "song", song_id, "album", album_id)
        self.add_relation(
            "song_artist", "song", song_id, "artist", artist_id)
        self.add_relation(
            "album_artist", "album", album_id, "artist", artist_id)

    def add_song(self, song_dict: dict) -> uuid.UUID:
        """Add a song to the song table of the database.

        Args:
            song_dict (dict): Song data, e.g. album, artist, rhythm data, audio features

        Returns:
            uuid.UUID | None: The UUID of the inserted song
        """
        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                # Check whether the song already exists
                columns = ["title", "happy_non_happy", "sad_non_sad"]
                if song_id := self.is_row_in_table("song", columns, song_dict):
                    return song_id

                # Song does not exist so we want to insert it into the song table
                try:
                    # Get the column names from song table
                    cur.execute("SELECT * FROM song LIMIT 0;")
                    column_names: list = [description[0]
                                          for description in cur.description]
                    sql_column_names: str = f'({", ".join(column_names)})'
                    sql_values_placeholders: str = f'({", ".join(["%s" for _ in column_names])})'
                    sql_insert_statement: str = f""" 
                                                    INSERT INTO song {sql_column_names}
                                                    VALUES {sql_values_placeholders}
                                                    RETURNING id;
                                                """

                    # Select the correct values to insert into database
                    sql_insert_values = [uuid.uuid4()]

                    for key, value in song_dict.items():
                        if key not in ["album", "artist", "date", "tracknumber"]:
                            if key == "valence_arousal":
                                sql_insert_values.append(value[0])
                                sql_insert_values.append(value[1])
                            else:
                                sql_insert_values.append(value)

                    cur.execute(sql_insert_statement, sql_insert_values)
                    inserted_row: tuple = cur.fetchone()

                    # Return song_id
                    if inserted_row:
                        song_id: uuid.UUID = inserted_row[0]
                        print(
                            f'Adding the song {song_dict["title"]} with id {song_id} to the database successful!')
                        return song_id
                    return None
                except Exception as e:
                    print(
                        f'Adding the song {song_dict["title"]} did not work:', e)
                    return None

    def add_album(self, song_dict: dict) -> uuid.UUID:
        """Add an album to the album table of the PostgreSQL database.

        Args:
            song_dict (dict): Song data, e.g. album, artist, rhythm data, audio features

        Returns:
            uuid.UUID | None: The UUID of the inserted album
        """
        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                # Check whether the album already exists
                columns = ["album", "date"]
                if album_id := self.is_row_in_table("album", columns, song_dict):
                    return album_id

                # Album does not exist so insert into the album table
                try:
                    sql_statement: str = """ 
                                            INSERT INTO album (title, date)
                                            VALUES (%s, %s)
                                            RETURNING id;
                                        """
                    sql_values = [song_dict["album"], date(
                        int(song_dict["date"]), 1, 1)]
                    cur.execute(sql_statement, sql_values)
                    inserted_row: tuple = cur.fetchone()

                    if inserted_row:
                        album_id: uuid.UUID = inserted_row[0]
                        print(
                            f'Adding the album {song_dict["album"]} with id {album_id} to the database was successful!')
                        return album_id
                    else:
                        print(
                            f'The album {song_dict["album"]} already exists')
                        return None
                except Exception as e:
                    print(
                        f'Adding an album {song_dict["album"]} did not work:', e)
                    return None

    def add_artist(self, song_dict: dict) -> uuid.UUID:
        """Add an artist to the artist table of the PostgreSQL database.

        Args:
            song_dict (dict): Song data, e.g. album, artist, rhythm data, audio features

        Returns:
            uuid.UUID | None: The UUID of the inserted artist
        """
        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                # Check whether artist already exists
                columns = ["artist"]
                if artist_id := self.is_row_in_table("artist", columns, song_dict):
                    return artist_id

                # Artist does not exist so insert into the artist table
                try:
                    sql_statement: str = """
                                            INSERT into artist (name)
                                            VALUES (%s)
                                            RETURNING id;
                                         """
                    sql_values = [song_dict["artist"]]
                    cur.execute(sql_statement, sql_values)
                    inserted_row: tuple = cur.fetchone()

                    if inserted_row:
                        artist_id: uuid.UUID = inserted_row[0]
                        print(
                            f'Adding the artist {song_dict["artist"]} with id {artist_id} to the database was successful!')
                        return artist_id
                    else:
                        print(
                            f'The artist {song_dict["artist"]} already exists!')
                        return None
                except Exception as e:
                    print(
                        f'Adding artist {song_dict["artist"]} did not work:', e)
                    return None

    def add_relation(self, rel_table: str, first_table: str, first_id: uuid.UUID, second_table: str, second_id: uuid.UUID) -> uuid.UUID:
        """Add a relationship into the relational table, e.g. song_id and album_id into song_album table.

        Args:
            rel_table (dict): Name of the relational table, e.g. song_album
            first_table (str): Name of the first table, e.g. song
            first_id (uuid.UUID): UUID of the first table row to insert, e.g. song_id
            second_table (str): Name of the second table, e.g. album
            second_id (uuid.UUID): UUID of the second table row to insert, e.g. album_id
            song_dict (dict): All provided song information

        Returns:
            uuid.UUID | None: The UUID of the inserted artist
        """
        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                # Check whether there already is a relation between first_id and second_id
                if relation_id := self.is_relation_in_table(rel_table, first_table, first_id, second_table, second_id):
                    return relation_id

                # The relation does not exist so insert it into the relation table
                try:
                    sql_statement: str = f"""
                                            INSERT into {rel_table} ({first_table}_id, {second_table}_id)
                                            VALUES (%s, %s)
                                            RETURNING id;
                                         """
                    sql_values = [first_id, second_id]
                    cur.execute(sql_statement, sql_values)

                    inserted_row: tuple = cur.fetchone()
                    if inserted_row:
                        rel_table_id: uuid.UUID = inserted_row[0]
                        print(
                            f'Adding the relation between {first_table} and {second_table} into {rel_table_id} successful')
                        return rel_table_id
                    else:
                        print(
                            f'The relationship {rel_table} already exists')
                except Exception as e:
                    print(e)

    def is_row_in_table(self, table_name: str, columns: list[str], song_dict: dict) -> uuid.UUID:
        """Check whether a row is already in a table. If yes return its UUID.

        Args:
            table_name (str): Name of the table, e.g. song
            columns (list[str]): Columns for the WHERE condition, e.g. title, happy_not_happy, danceable_not_danceable
            song_dict (dict): The data on which we check whether it is in the database

        Returns:
            uuid.UUID: If the row is in the table we return its UUID
        """
        sql_columns_mapping = {
            "album": "title",
            "artist": "name"
        }

        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                try:
                    # Create sql statement, e.g. "SELECT id FROM..." and condition, e.g. "WHERE title = %s AND date = %s"
                    conditions = []

                    for column in columns:
                        if column in sql_columns_mapping:
                            conditions.append(
                                f"{sql_columns_mapping[column]} = %s")
                        else:
                            conditions.append(f"{column} = %s")

                    sql_condition: str = " AND ".join(conditions)
                    sql_statement: str = f"SELECT id FROM {table_name} WHERE {sql_condition}"

                    # Get the values for the sql WHERE condition
                    sql_values: list = []
                    for key in song_dict:
                        if key in columns:
                            # Check for date objects
                            if key == "date":
                                sql_values.append(
                                    date(int(song_dict[key]), 1, 1))
                            else:
                                sql_values.append(song_dict[key])

                    cur.execute(sql_statement, sql_values)
                    result: tuple = cur.fetchone()

                    if result:
                        print(
                            f'The {table_name} with the column {song_dict[columns[0]]} already exists.')
                        id: uuid.UUID = result[0]
                        return id
                    return None
                except Exception as e:
                    print("An error occurred while looking at the database:", e)
                    return None

    def is_relation_in_table(self, rel_table: str, first_table: str, first_id: uuid.UUID, second_table: str, second_id: uuid.UUID) -> uuid.UUID:
        """Check whether a relation is already in the relation table (e.g. song_artist). If yes return its UUID.

        Args:
            rel_table (str): Name of the relational table, e.g. song_artist
            first_table (str): Name of the first table, e.g. song
            first_id (uuid.UUID): UUID primary key of the first table, e.g. song_id
            second_table (str): Name of the second table, e.g. artist
            second_id (uuid.UUID): UUID of the second table, e.g. artist_id

        Returns:
            uuid.UUID: UUID of the relationship, e.g. between a song and an artist
        """

        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                try:
                    sql_statement: str = f"SELECT id FROM {rel_table} WHERE {first_table}_id = %s AND {second_table}_id = %s;"
                    sql_values: list = [first_id, second_id]
                    cur.execute(sql_statement, sql_values)
                    result: tuple = cur.fetchone()

                    if result:
                        print(
                            f'The relation between {first_table}:{first_id} and {second_table}:{second_id} already exists.')
                        id: uuid.UUID = result[0]
                        return id
                    return None
                except Exception as e:
                    print(
                        f"An error occurred while looking at the {rel_table}:", e)
                    return None

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
