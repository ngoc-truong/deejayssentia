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
        5.886054, 5.5037227), 'danceable_not_danceable': 0.19786096256684493, 'aggressive_non_aggressive': 0.0, 'happy_non_happy': 0.09090909090909091, 'party_non_party': 0.7165775401069518, 'relaxed_non_relaxed': 0.0, 'sad_non_sad': 0.18085106382978725, 'acoustic_non_acoustic': 0.475177304964539, 'electronic_non_electronic': 0.0, 'instrumental_voice': 0.3333333333333333, 'female_male': 0.19858156028368795, 'bright_dark': 0.014184397163120567, 'acoustic_electronic': 0.014184397163120567, 'dry_wet': 0.7553191489361702}
    return song_dict


@pytest.fixture
def long_song_dict() -> dict:
    long_song_dict: dict = {'album': 'Hello Dolly (Remastered)', 'artist': 'Louis Armstrong', 'author': 'Louis Amstrong', 'composer': 'Louis Amstrong', 'copyright': '© 2014 Caribe Sound', 'date': '2014-12-10', 'discnumber': '1', 'isrc': 'ES6601404354', 'lyrics': "Cold empty bed, springs hard as lead\r\nFeel like old Ned, wished I was dead\r\nWhat did I do to be so black and blue?\r\n \r\nEven the mouse ran from my house\r\nThey laugh at you, and scorn you too\r\nWhat did I do to be so black and blue?\r\n \r\nI'm white inside, but that don't help my case\r\n'Cause I can't hide what is in my face\r\n \r\nHow would it end? Ain't got a friend\r\nMy only sin is in my skin\r\nWhat did I do to be so black and blue?\r\n\r\nHow would it end? Ain't got a friend\r\nMy only sin is in my skin\r\nWhat did I do to be so black and blue?",
                            'main_artist': 'Louis Amstrong', 'rating': 'Clean', 'title': 'Black and Blue (Remastered)', 'tracknumber': '3', 'valence_arousal': (5.1433234, 4.6531215), 'danceable_not_danceable': 0.005319148936170213, 'aggressive_non_aggressive': 0.0, 'happy_non_happy': 0.010638297872340425, 'party_non_party': 0.9308510638297872, 'relaxed_non_relaxed': 0.0035335689045936395, 'sad_non_sad': 0.01060070671378092, 'acoustic_non_acoustic': 0.24734982332155478, 'electronic_non_electronic': 0.0, 'instrumental_voice': 0.08833922261484099, 'female_male': 0.6996466431095406, 'bright_dark': 0.0, 'acoustic_electronic': 0.0, 'dry_wet': 0.6325088339222615, 'bpm': 99.64744567871094}
    return long_song_dict


@pytest.fixture
def db_connector() -> DBConnector:
    db_connector: DBConnector = DBConnector(os.getenv("DB_NAME"),
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
    env_string: str = f"dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} host={os.getenv('DB_HOST')} port={os.getenv('DB_PORT')}"
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


def test_add_data_will_increase_number_of_rows(table_names: list[str], song_dict: dict, env_as_str: str, db_connector: DBConnector, create_and_clean_database: pytest.fixture) -> None:
    """Test whether the number of rows will increase by 1 after inserting into tables.

    Args:
        table_names (list[str]): Names of tables, e.g. song, artist, album
        song_dict (dict): Data of the song, e.g. danceabe_non_dancable, arousal, valence
        env_as_str (str): Environment variables
        db_connector (DBConnector): An instance of the DBConnector class
        create_and_clean_database (pytest.fixture): Pytest Fixture to create the tables and clean the tables after the test was done
    """
    with psycopg.connect(env_as_str) as conn:
        with conn.cursor() as cur:
            # Check number of rows at beginning
            old_counts: list[int] = get_num_of_rows(table_names, cur)

            # Add data to all tables
            db_connector.add_data(song_dict)

            # Check number of rows again
            new_counts: list[int] = get_num_of_rows(table_names, cur)

            assert old_counts != new_counts
            for index, count in enumerate(new_counts):
                assert count == old_counts[index] + 1


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
    num_rows = []

    # Check previous number of rows
    for table in table_names:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            num_rows_in_table: list[int] = cur.fetchone()[0]
        except Exception as e:
            print("Selecting rows did not work.", e)
        else:
            num_rows.append(num_rows_in_table)
    return num_rows


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
