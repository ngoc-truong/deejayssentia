import os
from dotenv import load_dotenv

from audio_analyzer.PlaylistAnalyzer import PlaylistAnalyzer
from db.DBConnector import DBConnector

load_dotenv()

if __name__ == "__main__":
    folder_path = "/Users/ntruong/Documents/Personal/Programming/Projects/deejayssentia/music"
    playlist_analyzer = PlaylistAnalyzer(folder_path)
    song_infos: list = playlist_analyzer.get_all_song_info()

    # Database interaction
    table_names = ["album", "album_artist", "artist",
                   "song", "song_album", "song_artist"]
    db_connector: DBConnector = DBConnector(os.getenv("DB_NAME"),
                                            os.getenv("DB_USER"),
                                            os.getenv("DB_PASSWORD"),
                                            os.getenv("DB_HOST"),
                                            os.getenv("DB_PORT"))

    db_connector.delete_all_entries(table_names)
    for info in song_infos:
        db_connector.add_data(info)
