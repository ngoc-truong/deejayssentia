from pathlib import Path

from .AudioAnalyzer import AudioAnalyzer


class PlaylistAnalyzer:
    """ A class to analyze playlists or several audio files all at once. 
        It uses the AudioAnalyzer to analyze one audio file and aggregates the results.
    """

    def __init__(self, folder_path: str | Path):
        # This is a relative path, we always need the absolute path
        self.__folder_path: str | Path = None
        self.folder_path: str | Path = folder_path

    @property
    def folder_path(self) -> Path:
        return self.__folder_path

    @folder_path.setter
    def folder_path(self, new_path: str | Path) -> None:
        """Setter which checks whether the new_path is an absolute or relative path.

        Args:
            new_path (str | Path): The folder path provided by the user
        """
        if not Path(new_path).is_absolute():
            print(f"The folder path must be absolute.")
            return None
        else:
            self.__folder_path = Path(new_path)

    def get_filenames(self) -> list[str]:
        """Get filenames in a folder path

        Returns:
            list[str]: All files in the folder listed as strings
        """
        filenames = [
            file.name for file in self.__folder_path.iterdir() if file.is_file()]
        return filenames

    def get_all_song_info(self) -> list[dict]:

        filenames: list[str] = self.get_filenames()

        song_infos = []

        for name in filenames:
            audio_analyzer = AudioAnalyzer(
                f"{Path.joinpath(self.folder_path, name)}")
            song_info: dict = audio_analyzer.get_complete_song_info()
            song_infos.append(song_info)

        return song_infos
