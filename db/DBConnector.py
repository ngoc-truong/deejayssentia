import psycopg
import re

from datetime import date
from dotenv import load_dotenv
from uuid import uuid4, UUID

load_dotenv()


class DBConnector:
    """A class to interact with a PostgreSQL database (e.g. create, drop tables, insert into etc.).
    """

    def __init__(self, db_name: str, db_user: str, db_password: str, db_host: str, db_port: str):
        self.__DB_NAME: str = db_name
        self.__DB_USER: str = db_user
        self.__DB_PASSWORD: str = db_password
        self.__DB_HOST: str = db_host
        self.__DB_PORT: str = db_port

    def create_tables(self) -> None:
        """Create tables for song, artist and album.
        """
        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("BEGIN;")
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
                                    dry_wet FLOAT,
                                    bpm FLOAT
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
                    print(
                        "Creating database tables was succesfull or tables already exist!")
                except Exception as e:
                    print("Creating database tables did not work:", e)
                    conn.rollback()

    def drop_tables(self, table_names: list) -> None:
        """Drop all tables provided

        Args:
            table_names (list): List of table names to be dropped
        """
        for table in table_names:
            self.drop_table(table)

    def drop_table(self, table_name: str) -> None:
        """Drop a table in the PostgreSQL database.
        """
        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("BEGIN;")
                    cur.execute(
                        f"""DROP TABLE IF EXISTS {table_name} CASCADE;""")
                    conn.commit()
                except Exception as e:
                    print(f"Dropping the table {table_name} did not work:", e)
                    conn.rollback()

    def add_data(self, song_dict: dict) -> None:
        """Add all song data (e.g. album, artist, song and many-to-many relationships) into the PostgreSQL tables, e.g. song, album, artist.

        Args:
            song_dict (dict): Song data, e.g. album, artist, rhythm data, audio features
        """
        song_id: UUID = self.add_song(song_dict)
        album_id: UUID = self.add_album(song_dict)
        artist_id: UUID = self.add_artist(song_dict)

        self.add_relation(
            "song_album", "song", song_id, "album", album_id)
        self.add_relation(
            "song_artist", "song", song_id, "artist", artist_id)
        self.add_relation(
            "album_artist", "album", album_id, "artist", artist_id)

    def add_song(self, song_dict: dict) -> UUID | None:
        """Add a song to the song table of the database.

        Args:
            song_dict (dict): Song data, e.g. album, artist, rhythm data, audio features

        Returns:
            UUID | None: The UUID of the inserted song
        """
        # Check whether the song already exists
        columns = ["title", "happy_non_happy", "sad_non_sad"]

        if song_id := self.is_row_in_table("song", columns, song_dict):
            return song_id

        # Song does not exist so we want to insert it into the song table
        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("BEGIN;")
                    cur.execute("SELECT * FROM song LIMIT 0;")
                except Exception as e:
                    print("An error occurred while connecting to the song table:", e)
                    conn.rollback()
                    return None
                else:
                    # Get column names from the song table
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
                    sql_insert_values = [uuid4()]

                    for key, value in song_dict.items():
                        if key not in ("album", "artist", "date", "tracknumber"):
                            if key == "valence_arousal":
                                sql_insert_values.append(value[0])
                                sql_insert_values.append(value[1])
                            else:
                                sql_insert_values.append(value)

                try:
                    cur.execute("BEGIN;")
                    cur.execute(sql_insert_statement, sql_insert_values)
                    song_id = cur.fetchone()[0]
                except Exception as e:
                    print(
                        f'Adding the song "{song_dict["title"]}" did not work:', e)
                    conn.rollback()
                else:
                    print(
                        f'Adding the song "{song_dict["title"]}" with id "{song_id}" to the database successful!')
                    return song_id

    def add_album(self, song_dict: dict) -> UUID | None:
        """Add an album to the album table of the PostgreSQL database.

        Args:
            song_dict (dict): Song data, e.g. album, artist, rhythm data, audio features

        Returns:
            UUID | None: The UUID of the inserted album
        """
        # Check whether the album already exists
        columns = ["album", "date"]
        if album_id := self.is_row_in_table("album", columns, song_dict):
            return album_id

        # Album does not exist so insert into the album table
        sql_statement: str = """
                                INSERT INTO album (title, date)
                                VALUES (%s, %s)
                                RETURNING id;
                            """
        if "album" not in song_dict.keys():
            print(
                f'The key "album" does not exist in the dictionary: {song_dict}')
            return None

        try:
            year: int = int(song_dict["date"])
        except ValueError as e:
            print(f'Check the date: {song_dict["date"]}:', e)
            return None

        sql_values = [song_dict["album"], date(year, 1, 1)]

        # Database connection
        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("BEGIN;")
                    cur.execute(sql_statement, sql_values)
                    album_id: tuple = cur.fetchone()[0]
                except Exception as e:
                    print(f"An error occured while connecting to the database:", e)
                    conn.rollback()
                else:
                    print(
                        f'Adding the album "{song_dict["album"]}" with id "{album_id}" to the database was successful!')
                    return album_id

    def add_artist(self, song_dict: dict) -> UUID | None:
        """Add an artist to the artist table of the PostgreSQL database.

        Args:
            song_dict (dict): Song data, e.g. album, artist, rhythm data, audio features

        Returns:
            UUID | None: The UUID of the inserted artist
        """
        # Check whether artist already exists
        columns = ["artist"]
        if artist_id := self.is_row_in_table("artist", columns, song_dict):
            return artist_id

        # Artist does not exist so insert into the artist table
        sql_statement: str = """
                                INSERT into artist (name)
                                VALUES (%s)
                                RETURNING id;
                                """

        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                try:
                    sql_values = [song_dict["artist"]]
                except KeyError as e:
                    print(f'The key "artist" does not exist.')
                    return None

                try:
                    cur.execute("BEGIN;")
                    cur.execute(sql_statement, sql_values)
                    artist_id: tuple = cur.fetchone()[0]
                except Exception as e:
                    print("Could not execute sql statement:", sql_statement)
                    conn.rollback()
                else:
                    print(
                        f'Adding the artist "{song_dict["artist"]}" with id "{artist_id}" to the database was successful!')
                    return artist_id

    def add_relation(self, rel_table: str, first_table: str, first_id: UUID, second_table: str, second_id: UUID) -> UUID:
        """Add a relationship into the relational table, e.g. song_id and album_id into song_album table.

        Args:
            rel_table (dict): Name of the relational table, e.g. song_album
            first_table (str): Name of the first table, e.g. song
            first_id (UUID): UUID of the first table row to insert, e.g. song_id
            second_table (str): Name of the second table, e.g. album
            second_id (UUID): UUID of the second table row to insert, e.g. album_id
            song_dict (dict): All provided song information

        Returns:
            UUID | None: The UUID of the inserted artist
        """
        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                # Check whether there already is a relation between first_id and second_id
                if relation_id := self.is_relation_in_table(rel_table, first_table, first_id, second_table, second_id):
                    return relation_id

                # The relation does not exist so insert it into the relation table
                sql_statement: str = f"""
                                        INSERT into {rel_table} ({first_table}_id, {second_table}_id)
                                        VALUES (%s, %s)
                                        RETURNING id;
                                        """
                sql_values = [first_id, second_id]

                try:
                    cur.execute("BEGIN;")
                    cur.execute(sql_statement, sql_values)
                    rel_table_id: tuple = cur.fetchone()[0]
                except Exception as e:
                    print("An error occured while inserting into the database:", e)
                    conn.rollback()
                    return None
                else:
                    print(
                        f'Adding the relation between {first_table} and {second_table} into {rel_table_id} successful')
                    return rel_table_id

    def is_row_in_table(self, table_name: str, columns: list[str], song_dict: dict) -> UUID | None:
        """Check whether a row is already in a table. If yes return its UUID.

        Args:
            table_name (str): Name of the table, e.g. song
            columns (list[str]): Columns for the WHERE condition, e.g. title, happy_not_happy, danceable_not_danceable
            song_dict (dict): The data on which we check whether it is in the database

        Returns:
            UUID: If the row is in the table we return its UUID
        """
        sql_columns_mapping = {
            "album": "title",
            "artist": "name"
        }

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

        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("BEGIN;")
                    cur.execute(sql_statement, sql_values)
                except Exception as e:
                    print("An error occurred while looking at the database:", e)
                    conn.rollback()
                    return None
                else:
                    result: tuple = cur.fetchone()
                    if result:
                        print(
                            f'The {table_name} with the column {song_dict[columns[0]]} already exists.')
                        id: UUID = result[0]
                        return id
                    return None

    def is_relation_in_table(self, rel_table: str, first_table: str, first_id: UUID, second_table: str, second_id: UUID) -> UUID | None:
        """Check whether a relation is already in the relation table (e.g. song_artist). If yes return its UUID.

        Args:
            rel_table (str): Name of the relational table, e.g. song_artist
            first_table (str): Name of the first table, e.g. song
            first_id (UUID): UUID primary key of the first table, e.g. song_id
            second_table (str): Name of the second table, e.g. artist
            second_id (UUID): UUID of the second table, e.g. artist_id

        Returns:
            UUID: UUID of the relationship, e.g. between a song and an artist
        """
        sql_statement: str = f"SELECT id FROM {rel_table} WHERE {first_table}_id = %s AND {second_table}_id = %s;"
        sql_values: list = [first_id, second_id]

        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("BEGIN;")
                    cur.execute(sql_statement, sql_values)
                except Exception as e:
                    conn.rollback()
                    print(
                        f"An error occurred while selecting an entry from the {rel_table} table:", e)
                else:
                    result: tuple = cur.fetchone()
                    if result:
                        print(
                            f'The relation between "{first_table}:{first_id}" and "{second_table}:{second_id}" already exists.')
                        id: UUID = result[0]
                        return id
                    return None

    def delete_all_entries(self, table_names: list[str]) -> None:
        """Delete all entries in provided tables.

        Args:
            table_names (list[str]): Names of the tables
        """
        for table in table_names:
            self.delete_all_entries_in_table(table)

    def delete_all_entries_in_table(self, table_name: str) -> None:
        """Delete all entries from a table, e.g. all rows in "song" table.

        Args:
            table_name (str): Name of the table, e.g. "song"
        """
        with psycopg.connect(f"dbname={self.__DB_NAME} user={self.__DB_USER} password={self.__DB_PASSWORD} host={self.__DB_HOST} port={self.__DB_PORT}") as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("BEGIN")
                    cur.execute(f"DELETE FROM {table_name};")
                except Exception as e:
                    conn.rollback()
                    print(f'Could not delete entries in "{table_name}":', e)

    def get_correct_date_format(date_string: str) -> date | None:
        """Get the correct date format, e.g. if only a "1999" is given, it will become
           an date object with 1999-01-01.

        Args:
            date_string (str): A date string, e.g. "1999"

        Returns:
            date | None: A date object
        """

        year_pattern = r'^\d{4}$'
        year_month_pattern = r'^\d{4}-\d{2}$'
        year_month_day_pattern = r'^\d{4}-\d{2}-\d{2}$'

        if re.match(year_month_day_pattern, date_string):
            year, month, day = date_string.split("-")
            return date(int(year), int(month), int(day))
        elif re.match(year_month_pattern, date_string):
            year, month = date_string.split("-")
            return date(int(year), int(month), 1)
        elif re.match(year_pattern, date_string):
            return date(int(date_string), 1, 1)
        else:
            print(f'Date format: f"{date_string}"is invalid.')
            return None
