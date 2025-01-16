import os
import psycopg
import pytest

from datetime import date
from dotenv import load_dotenv
from psycopg import Cursor
from uuid import UUID

from db.DBConnector import DBConnector

load_dotenv()


@pytest.fixture
def song_dict() -> dict:
    song_dict: dict = {'album': 'Satch Plays Fats', 'artist': 'Louis Armstrong', 'comment:n': 'Converted by https://spotifydown.com', 'date': '1955', 'title': "I'm Crazy 'Bout My Baby - Edit", 'tracknumber': '', 'valence_arousal': (
        5.886054, 5.5037227), 'danceable_not_danceable': 0.19786096256684493, 'aggressive_non_aggressive': 0.0, 'happy_non_happy': 0.09090909090909091, 'party_non_party': 0.7165775401069518, 'relaxed_non_relaxed': 0.0, 'sad_non_sad': 0.18085106382978725, 'acoustic_non_acoustic': 0.475177304964539, 'electronic_non_electronic': 0.0, 'instrumental_voice': 0.3333333333333333, 'female_male': 0.19858156028368795, 'bright_dark': 0.014184397163120567, 'acoustic_electronic': 0.014184397163120567, 'dry_wet': 0.7553191489361702, 'bpm': 120}
    return song_dict


@pytest.fixture
def long_song_dict() -> dict:
    long_song_dict: dict = {'album': 'Hello Dolly (Remastered)', 'artist': 'Louis Armstrong', 'author': 'Louis Amstrong', 'composer': 'Louis Amstrong', 'copyright': '© 2014 Caribe Sound', 'date': '2014-12-10', 'discnumber': '1', 'isrc': 'ES6601404354', 'lyrics': "Cold empty bed, springs hard as lead\r\nFeel like old Ned, wished I was dead\r\nWhat did I do to be so black and blue?\r\n \r\nEven the mouse ran from my house\r\nThey laugh at you, and scorn you too\r\nWhat did I do to be so black and blue?\r\n \r\nI'm white inside, but that don't help my case\r\n'Cause I can't hide what is in my face\r\n \r\nHow would it end? Ain't got a friend\r\nMy only sin is in my skin\r\nWhat did I do to be so black and blue?\r\n\r\nHow would it end? Ain't got a friend\r\nMy only sin is in my skin\r\nWhat did I do to be so black and blue?",
                            'main_artist': 'Louis Amstrong', 'rating': 'Clean', 'title': 'Black and Blue (Remastered)', 'tracknumber': '3', 'valence_arousal': (5.1433234, 4.6531215), 'danceable_not_danceable': 0.005319148936170213, 'aggressive_non_aggressive': 0.0, 'happy_non_happy': 0.010638297872340425, 'party_non_party': 0.9308510638297872, 'relaxed_non_relaxed': 0.0035335689045936395, 'sad_non_sad': 0.01060070671378092, 'acoustic_non_acoustic': 0.24734982332155478, 'electronic_non_electronic': 0.0, 'instrumental_voice': 0.08833922261484099, 'female_male': 0.6996466431095406, 'bright_dark': 0.0, 'acoustic_electronic': 0.0, 'dry_wet': 0.6325088339222615, 'bpm': 99.64744567871094}
    return long_song_dict


@pytest.fixture
def db_connector() -> DBConnector:
    db_connector: DBConnector = DBConnector(os.getenv("DB_TEST_NAME"),
                                            os.getenv("DB_USER"),
                                            os.getenv("DB_PASSWORD"),
                                            os.getenv("DB_HOST"),
                                            os.getenv("DB_PORT"))
    return db_connector


@pytest.fixture
def env_as_str() -> str:
    """Return the environment variables to connect to the database.

    Returns:
        str: environment variables as a string
    """
    env_string: str = f"dbname={os.getenv('DB_TEST_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} host={os.getenv('DB_HOST')} port={os.getenv('DB_PORT')}"
    return env_string


@pytest.fixture
def table_names() -> list:
    """Return the names of the tables, e.g. song, artist, album.

    Returns:
        list: Name of tables
    """
    table_names: list = ["album", "album_artist", "artist",
                         "song", "song_album", "song_artist"]
    return table_names


@pytest.fixture
def create_and_clean_database(db_connector: DBConnector, table_names: list[str]):
    """Fixture to create and clean all tables before/after the tests will run. Therefore we always have a clean test database.

    Args:
        db_connector (DBConnector): Instance of DBConnector class
        table_names (list[str]): Names of tables, e.g. song, artist, album
    """
    db_connector.create_tables()

    yield

    db_connector.delete_all_entries(table_names)
    db_connector.drop_tables(table_names)


def test_same_x_will_not_increase_x_table_but_x_y_table(table_names: list[str], env_as_str: str, db_connector: DBConnector, create_and_clean_database: pytest.fixture):
    """Test if a row, e.g. artist already exists, inserting it will not work (no increase in row numbers) but the x_y_table (e.g. song_artist table) will increase.

    Args:
        table_names (list[str]): Names of tables, e.g. song, artist, album
        env_as_str (str): Environment variables
        db_connector (DBConnector): An instance of the DBConnector class
        create_and_clean_database (pytest.fixture): Pytest Fixture to create the tables and clean the tables after the test was done
    """
    original_data: dict = {'album': 'Satch Plays Fats', 'artist': 'Louis Armstrong', 'comment:n': 'Converted by https://spotifydown.com', 'date': '1955', 'title': "I'm Crazy 'Bout My Baby - Edit", 'tracknumber': '', 'valence_arousal': (
        5.886054, 5.5037227), 'danceable_not_danceable': 0.19786096256684493, 'aggressive_non_aggressive': 0.0, 'happy_non_happy': 0.09090909090909091, 'party_non_party': 0.7165775401069518, 'relaxed_non_relaxed': 0.0, 'sad_non_sad': 0.18085106382978725, 'acoustic_non_acoustic': 0.475177304964539, 'electronic_non_electronic': 0.0, 'instrumental_voice': 0.3333333333333333, 'female_male': 0.19858156028368795, 'bright_dark': 0.014184397163120567, 'acoustic_electronic': 0.014184397163120567, 'dry_wet': 0.7553191489361702, 'bpm': 120}
    same_artist_data: dict = {'album': 'Other Album', 'artist': 'Louis Armstrong', 'comment:n': 'Converted by https://spotifydown.com', 'date': '1955', 'title': "Other Song", 'tracknumber': '', 'valence_arousal': (
        5.886054, 5.5037227), 'danceable_not_danceable': 0.19786096256684493, 'aggressive_non_aggressive': 0.0, 'happy_non_happy': 0.09090909090909091, 'party_non_party': 0.7165775401069518, 'relaxed_non_relaxed': 0.0, 'sad_non_sad': 0.18085106382978725, 'acoustic_non_acoustic': 0.475177304964539, 'electronic_non_electronic': 0.0, 'instrumental_voice': 0.3333333333333333, 'female_male': 0.19858156028368795, 'bright_dark': 0.014184397163120567, 'acoustic_electronic': 0.014184397163120567, 'dry_wet': 0.7553191489361702, 'bpm': 120}
    same_album_data: dict = {'album': 'Satch Plays Fats', 'artist': 'Other Artist', 'comment:n': 'Converted by https://spotifydown.com', 'date': '1955', 'title': "Other Song", 'tracknumber': '', 'valence_arousal': (
        5.886054, 5.5037227), 'danceable_not_danceable': 0.19786096256684493, 'aggressive_non_aggressive': 0.0, 'happy_non_happy': 0.09090909090909091, 'party_non_party': 0.7165775401069518, 'relaxed_non_relaxed': 0.0, 'sad_non_sad': 0.18085106382978725, 'acoustic_non_acoustic': 0.475177304964539, 'electronic_non_electronic': 0.0, 'instrumental_voice': 0.3333333333333333, 'female_male': 0.19858156028368795, 'bright_dark': 0.014184397163120567, 'acoustic_electronic': 0.014184397163120567, 'dry_wet': 0.7553191489361702, 'bpm': 120}
    same_song_data: dict = {'album': 'Greatest Hits', 'artist': 'Louis Armstrong', 'comment:n': 'Converted by https://spotifydown.com', 'date': '1955', 'title': "I'm Crazy 'Bout My Baby - Edit", 'tracknumber': '', 'valence_arousal': (
        5.886054, 5.5037227), 'danceable_not_danceable': 0.19786096256684493, 'aggressive_non_aggressive': 0.0, 'happy_non_happy': 0.09090909090909091, 'party_non_party': 0.7165775401069518, 'relaxed_non_relaxed': 0.0, 'sad_non_sad': 0.18085106382978725, 'acoustic_non_acoustic': 0.475177304964539, 'electronic_non_electronic': 0.0, 'instrumental_voice': 0.3333333333333333, 'female_male': 0.19858156028368795, 'bright_dark': 0.014184397163120567, 'acoustic_electronic': 0.014184397163120567, 'dry_wet': 0.7553191489361702, 'bpm': 120}

    # same_x = artist, y = song: The same artist will not increase "artist" table but "song_artist" table
    with psycopg.connect(env_as_str) as conn:
        with conn.cursor() as cur:
            row_tester("artist", "song", same_artist_data,
                       original_data, db_connector, cur)

            db_connector.delete_all_entries(table_names)
            row_tester("album", "song", same_album_data,
                       original_data, db_connector, cur)

            db_connector.delete_all_entries(table_names)
            row_tester("song", "album", same_song_data,
                       original_data, db_connector, cur)


def row_tester(same_x: str, y: str, changed_data: dict, original_data: dict, db_connector: DBConnector, cur: Cursor) -> None:
    """ Helper test method for test_same_x_will_not_increase_x_table_but_x_y_table. 
        Test if a row, e.g. artist already exists, inserting it into artist table will not work (no increase in row numbers) but the x_y_table (e.g. song_artist table) will work (increase in row numbers).

    Args:
        same_x (str): The name of the entry which already exists, e.g. the artist "Louis Armstrong"
        y (str): The name of the entry which does not already exists, e.g. the songs "La vie en rose" and "What a beautiful world"
        changed_data (dict): Changed song data, e.g. where the song title changed from "La vie en rose" to "What a beautiful world"
        original_data (dict): Original song data
        db_connector (DBConnector): An instance of the DBConnector class
        cur (Cursor): An instance of Cursor
    """
    if same_x == "artist" and y == "song":
        first_y_id: UUID = db_connector.add_song(original_data)
        second_y_id: UUID = db_connector.add_song(changed_data)
        first_x_id: UUID = db_connector.add_artist(original_data)
        second_x_id: UUID = db_connector.add_artist(changed_data)
        table_name: str = f"{y}_{same_x}"
    elif same_x == "album" and y == "song":
        first_y_id: UUID = db_connector.add_song(original_data)
        second_y_id: UUID = db_connector.add_song(changed_data)
        first_x_id: UUID = db_connector.add_album(original_data)
        second_x_id: UUID = db_connector.add_album(changed_data)
        table_name: str = f"{y}_{same_x}"
    elif same_x == "song" and y == "album":
        first_y_id: UUID = db_connector.add_album(original_data)
        second_y_id: UUID = db_connector.add_album(changed_data)
        first_x_id: UUID = db_connector.add_song(original_data)
        second_x_id: UUID = db_connector.add_song(changed_data)
        table_name: str = f"{same_x}_{y}"

    assert first_y_id != second_y_id
    assert first_x_id == second_x_id

    rows: int = get_num_of_rows_for_table(y, cur)
    assert rows == 2

    rows: int = get_num_of_rows_for_table(same_x, cur)
    assert rows == 1

    db_connector.add_relation(table_name, y, first_y_id, same_x, first_x_id)
    db_connector.add_relation(table_name, y, second_y_id, same_x, second_x_id)
    rows: int = get_num_of_rows_for_table(table_name, cur)
    assert rows == 2


def test_row_already_exists(table_names: list[str], song_dict: dict, env_as_str: str, db_connector: DBConnector, create_and_clean_database: pytest.fixture) -> None:
    """Test if a row already exists (in all tables provided).

    Args:
        table_names (list[str]): Names of tables, e.g. song, artist, album
        song_dict (dict): Data of the song, e.g. danceabe_non_dancable, arousal, valence
        env_as_str (str): Environment variables
        db_connector (DBConnector): An instance of the DBConnector class
        create_and_clean_database (pytest.fixture): Pytest Fixture to create the tables and clean the tables after the test was done
    """
    with psycopg.connect(env_as_str) as conn:
        with conn.cursor() as cur:
            db_connector.add_data(song_dict)
            old_counts: list[int] = get_num_of_rows(table_names, cur)

            db_connector.add_data(song_dict)
            new_counts: list[int] = get_num_of_rows(table_names, cur)

            assert old_counts == new_counts


def test_row_already_exists_in_one_table(song_dict: dict, env_as_str: str, db_connector: DBConnector, create_and_clean_database: pytest.fixture) -> None:
    """Test in one table, e.g. song, whether there is already an entry and that the entry will not be overwritten by a new one.

    Args:
        song_dict (dict): Data of the song, e.g. danceabe_non_dancable, arousal, valence
        env_as_str (str): Environment variables
        db_connector (DBConnector): An instance of the DBConnector class
        create_and_clean_database (pytest.fixture): Pytest fixture to create the tables and clean the tables after the test was done
    """
    with psycopg.connect(env_as_str) as conn:
        with conn.cursor() as cur:
            song_id: UUID = db_connector.add_song(song_dict)
            num_of_rows = get_num_of_rows_where("id", song_id, "song", cur)

            # It will return the already existent song_id, see below
            failed_song_id = db_connector.add_song(song_dict)

            assert failed_song_id != None
            assert failed_song_id == song_id
            assert num_of_rows == 1


def test_relationship_works(song_dict: dict, env_as_str: str, db_connector: DBConnector, create_and_clean_database: pytest.fixture) -> None:
    """Test whether adding a relationship (e.g. song_artist) works and whether adding the same relationship will not be commited.

    Args:
        song_dict (dict): Data of the song, e.g. danceabe_non_dancable, arousal, valence
        env_as_str (str): Environment variables
        db_connector (DBConnector): An instance of the DBConnector class
        create_and_clean_database (pytest.fixture): Pytest fixture to create the tables and clean the tables after the test was done
    """
    first_table: str = "song"
    second_table: str = "artist"

    with psycopg.connect(env_as_str) as conn:
        with conn.cursor() as cur:
            # change method here for testing with other tables
            first_id = db_connector.add_song(song_dict)
            # change method here for testing with other tables
            second_id = db_connector.add_artist(song_dict)
            db_connector.add_relation(
                f"{first_table}_{second_table}", first_table, first_id, second_table, second_id)

            try:
                # sql_statement: str = f"SELECT * FROM {first_table}_{second_table} WHERE {first_table}_id = %s AND {second_table}_id = %s"
                # cur.execute(sql_statement, (first_id, second_id))
                sql_statement: str = f"SELECT * FROM {first_table}_{second_table}"
                cur.execute(sql_statement)
                relation_id = cur.fetchone()[0]

            except Exception as e:
                print("Selecting a row from the relationship table did not work:", e)
            else:
                assert relation_id != 0

                num_of_rows = get_num_of_rows_where(
                    "id", relation_id, f"{first_table}_{second_table}", cur)
                assert num_of_rows == 1

                # If another relation is added it won't work: the num_of_rows should still be 1
                db_connector.add_relation(
                    f"{first_table}_{second_table}", first_table, first_id, second_table, second_id)
                num_of_rows = get_num_of_rows_where(
                    "id", relation_id, f"{first_table}_{second_table}", cur)

                assert relation_id != 0
                assert num_of_rows == 1


def test_only_relevant_columns_will_be_inserted(song_dict: dict, long_song_dict: dict, db_connector, create_and_clean_database: pytest.fixture) -> None:
    """Test whether only the relevant columns will be inserted although longer dictionaries are provided.

    Args:
        song_dict (dict): Data of the song, e.g. danceabe_non_dancable, arousal, valence
        db_connector (DBConnector): An instance of the DBConnector class
        create_and_clean_database (pytest.fixture): Pytest fixture to create the tables and clean the tables after the test was done
    """

    first_id: UUID = db_connector.add_song(song_dict)
    second_id: UUID = db_connector.add_song(long_song_dict)

    assert first_id
    assert second_id


# Helper functions
def test_correct_date_format(db_connector) -> None:
    year_incorrectmonth_day: str = "1999-13-01"
    year_month_incorrectday: str = "1999-12-32"
    year_incorrectmonth: str = "1999-13"
    incorrectyear_long: str = "12345"
    incorrectyear_short: str = "123"
    incorrect: str = "Hello"

    incorrect_date_formats: list = [year_incorrectmonth_day, year_month_incorrectday,
                                    year_incorrectmonth, incorrectyear_long, incorrectyear_short, incorrect]

    for format in incorrect_date_formats:
        date_format = db_connector.get_correct_date_format(format)
        assert date_format == None

    # Test year_month_day, "YYYY-MM-DD"
    year_month_day = "1999-12-03"
    year_month = "1999-12"
    year = "1999"
    correct_date_formats: list = [year_month_day, year_month, year]

    for format in correct_date_formats:
        date_format: date = db_connector.get_correct_date_format(format)
        assert isinstance(date_format, date)
        assert date_format.year == 1999
        assert date_format.month == 12 or date_format.month == 1
        assert date_format.day == 3 or date_format.day == 1


def get_num_of_rows(table_names: list[str], cur: Cursor) -> list[int]:
    """Get the number of rows from provided tables

    Args:
        table_names (list[str]): Name of the database tables
        cur (Cursor): A psycopg cursor object

    Returns:
        list: Numbers of rows, e.g. [1, 3, 5, 1, 3, 5] if we have six tables
    """
    num_rows = [get_num_of_rows_for_table(table, cur) for table in table_names]
    return num_rows


def get_num_of_rows_for_table(table: str, cur: Cursor) -> int:
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        num_of_rows: int = int(cur.fetchone()[0])
    except Exception as e:
        print("Selecting rows did not work.", e)
    else:
        return num_of_rows


def get_num_of_rows_where(column_name: str, column_value: UUID | str, table_name: str, cur: Cursor) -> int:
    """Returns how many rows of a where condition, e.g. id = 3123 are in the table

    Args:
        column (UUID | str): The column to look for
        table_name (str): The table name
        cur (Cursor): A psycopg cursor object

    Returns
        int: The number of rows where a condition is met
    """
    try:
        cur.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} = %s;", (column_value, ))
        count: int = cur.fetchone()[0]
    except Exception as e:
        print("Selecting number of rows did not work:", e)
    else:
        return count
