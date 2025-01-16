import os
from dotenv import load_dotenv

from audio_analyzer.PlaylistAnalyzer import PlaylistAnalyzer
from audio_analyzer.AudioAnalyzer import AudioAnalyzer
from db.DBConnector import DBConnector

load_dotenv()

if __name__ == "__main__":
    # folder_path = "/Users/ntruong/Documents/Personal/Programming/Projects/deejayssentia/music"
    # playlist_analyzer = PlaylistAnalyzer(folder_path)
    # song_infos: list = playlist_analyzer.get_all_song_info()

    # # Database interaction
    # table_names = ["album", "album_artist", "artist",
    #                "song", "song_album", "song_artist"]
    # db_connector: DBConnector = DBConnector(os.getenv("DB_NAME"),
    #                                         os.getenv("DB_USER"),
    #                                         os.getenv("DB_PASSWORD"),
    #                                         os.getenv("DB_HOST"),
    #                                         os.getenv("DB_PORT"))
    # db_connector.delete_all_entries(table_names)
    # db_connector.drop_tables(table_names)
    # db_connector.create_tables()

    # for info in song_infos:
    #     db_connector.add_data(info)

    song_path = "/Users/ntruong/Documents/Personal/Programming/Projects/deejayssentia/music/sad_male_voice.flac"
    song2_path = "/Users/ntruong/Documents/Personal/Programming/Projects/deejayssentia/music/danceable.mp3"
    audio_analyzer = AudioAnalyzer(song_path)
    audio_analyzer_2 = AudioAnalyzer(song2_path)

    print(audio_analyzer.get_complete_song_info())
    print(audio_analyzer_2.get_complete_song_info())
