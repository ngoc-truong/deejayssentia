import json
import numpy as np

from essentia.standard import MonoLoader, MetadataReader, TensorflowPredictEffnetDiscogs, TensorflowPredict2D, TensorflowPredictMusiCNN, RhythmExtractor2013
from pathlib import Path, PosixPath, WindowsPath


class AudioAnalyzer:
    """ A class to analyze audio files, e.g. getting audio track features like valence, 
        danceability, aggressiveness.
    """

    def __init__(self, file_path: str | Path):
        self.__file_path: str = None        # Must be string to work with Essentia
        self.file_path: str = file_path     # Setter method

        script_dir: Path = Path(__file__).parent
        self.__model_path: Path = script_dir.joinpath("models")
        self.__audio: MonoLoader = self.__get_essentia_audio()
        self.__song_info: dict = None

    @property
    def file_path(self) -> str:
        return self.__file_path

    @file_path.setter
    def file_path(self, new_path: str | Path) -> None:
        if isinstance(new_path, str):
            abs_path: Path = Path(new_path).resolve()
        elif isinstance(new_path, Path) or isinstance(new_path, PosixPath) or isinstance(new_path, WindowsPath):
            abs_path: Path = new_path.resolve()

        if not abs_path.is_file() and abs_path.suffix:
            raise FileNotFoundError(f"The file '{abs_path}' does not exist.")

        if abs_path.suffix not in {".mp3", ".flac", ".wav", ".m4a", ".ogg", ".aiff"}:
            raise ValueError(f"No audio file was provided:", abs_path.suffix)

        self.__file_path: str = str(abs_path)

    @property
    def song_info(self):
        return self.__song_info

    @song_info.setter
    def song_info(self, new_song_info: dict) -> None:
        self.__song_info: dict = new_song_info

    def get_metadata(self) -> dict:
        """Get the metadata of a song.

        Returns:
            dict: key and value of the metadata, e.g. {"album": "The Wildest!"}
        """
        try:
            metadata_pool: MetadataReader = MetadataReader(
                filename=self.__file_path)()[7]
        except IndexError:
            print("Index out of bounty hunter.")

        metadata: dict = {}

        try:
            for descriptor in metadata_pool.descriptorNames():
                key: str = descriptor.split(".")[-1]
                metadata[key] = metadata_pool[descriptor][0]
        except IndexError:
            print("Index out of bounty hunter.")
        else:
            return metadata

    def get_rhythm_data(self) -> dict:
        """ Get the rhythm data of a song, e.g. bpm, beats, beats_confidence, _, 
            beats_intervals.

        Returns:
            dict: rhythm data, e.g. {"bpm": 140}
        """
        rhythm_extractor = RhythmExtractor2013(method="multifeature")
        try:
            bpm: float
            beats: np.ndarray
            beats_confidence: float
            beats_intervals: np.ndarray

            bpm, beats, beats_confidence, _, beats_intervals = rhythm_extractor(
                self.__audio)
            return {"bpm": bpm, "beats": beats, "beats_confidence": beats_confidence, "beats_intervals": beats_intervals}
        except Exception as e:
            print(
                f"An error occured while loading the rhythm data, did you clap on 1 and 3? {e}")

    def __get_essentia_audio(self) -> MonoLoader:
        """Load the file into an Essentia audio object

        Returns:
            MonoLoader: A MonoLoader object from essentia containing the audio
        """
        return MonoLoader(filename=self.__file_path,
                          sampleRate=16000, resampleQuality=4)()

    def __get_audio_feature_config(self) -> dict:
        """ Get the whole audio feature config for all audio features (e.g. danceability, 
            aggressiveness). It contains values like model (effnet, musicnn), algorithm 
            (regression, classifier) and file path names.
        Returns:
            dict: Configuration values for all audio features
        """
        script_dir: Path = Path(__file__).parent
        config_json: Path = script_dir.joinpath(
            "data", "audio_features_config.json")

        try:
            with open(config_json, "r") as file:
                parameters: dict = json.load(file)

            return parameters
        except FileNotFoundError:
            print(f"The file {config_json} could not be found, sadness!")

    def __get_single_audio_feature_config(self, audio_feature: str) -> dict:
        """Get the model name and graph filenames used for prediction of an audio feature.

        Returns:
            dict: Config parameters (e.g. graph filename) for one audio feature, e.g. danceability
        """
        parameters = self.__get_audio_feature_config()
        return parameters[audio_feature]

    def get_predictions(self, audio_feature: str) -> np.ndarray:
        """ Calculate predictions of an audio feature.
            See https://essentia.upf.edu/models.html for meaning of values, e.g. 
            first column happy, second column non_happy
        Args:
            audio_feature (str): Name of the audio feature, e.g. danceability

        Returns:
            np.ndarray: Predictions for each segment of a song
        """
        audio_config: dict = self.__get_single_audio_feature_config(
            audio_feature)

        embedding_graph_file_path: str = str(Path.joinpath(
            self.__model_path, audio_config["embedding_graph_filename"]))
        prediction_graph_file_path: str = str(Path.joinpath(
            self.__model_path, audio_config["prediction_graph_filename"]))

        # Create embeddings based on pre-trained model (e.g. musiccn, effnet)
        try:
            if audio_config["model"] == "musicnn":
                embedding_model: 'Algo' = TensorflowPredictMusiCNN(
                    graphFilename=embedding_graph_file_path, output="model/dense/BiasAdd")
            elif audio_config["model"] == "effnet":
                embedding_model: 'Algo' = TensorflowPredictEffnetDiscogs(
                    graphFilename=embedding_graph_file_path, output="PartitionedCall:1")
        except FileNotFoundError:
            print(
                f"Embedding graph file path not found: {embedding_graph_file_path}")

        embeddings: np.ndarray = embedding_model(self.__audio)

        # Create predictions based on algorithm (regression, classifier)
        if audio_config["algorithm"] == "regression":
            prediction_model: 'Algo' = TensorflowPredict2D(
                graphFilename=prediction_graph_file_path, output="model/Identity")
        elif audio_config["algorithm"] == "classifier":
            prediction_model: 'Algo' = TensorflowPredict2D(
                graphFilename=prediction_graph_file_path, output="model/Softmax")

        predictions: np.ndarray = prediction_model(embeddings)

        return predictions

    def calculate_prediction_metric(self, audio_feature: str, category: int = 0) -> tuple | float:
        """ Calculate a single prediction metric from several predictions 
            (for each segment in a song) e.g. a ratio for classifiers (e.g. 
            danceable/non-danceable ratio) or a mean for regression

        Args:
            audio_feature (str): Name of the audio feature
            category (int, optional): Only relevant for classifier. 
                0 means first category (e.g. danceable)
                1 means second category (e.g. non-danceable)

        Returns:
            tuple | float: _description_
        """
        audio_config: dict = self.__get_single_audio_feature_config(
            audio_feature)
        predictions: np.ndarray = self.get_predictions(audio_feature)
        probability_cutoff: float = 0.5

        if audio_config["algorithm"] == "regression":
            try:
                avg_predictions: np.ndarray = np.mean(predictions, axis=0)
                return tuple(avg_predictions)
            except Exception as e:
                print(e)
        elif audio_config["algorithm"] == "classifier":
            try:
                count: np.int64 = np.sum(
                    predictions[:, category] > probability_cutoff)
                ratio: float = float(count / len(predictions))
                return ratio
            except IndexError:
                print(f"Column {category} does not exist.")

    def get_complete_song_info(self) -> dict:
        """ Get all song information, namely metadata and audio feature 
            predictions (e.g. danceability) 

        Returns:
            dict: Metadata and song features (e.g. danceability)
        """
        # Metadata
        audio_info: dict = self.get_metadata()

        # Audio feature predictions
        audio_feature_config: dict = self.__get_audio_feature_config()
        audio_features = list(audio_feature_config.keys())
        predictions: dict = {feature: self.calculate_prediction_metric(
            feature) for feature in audio_features}
        audio_info.update(predictions)

        # Bpm
        bpm: float = float(self.get_rhythm_data()["bpm"])
        audio_info["bpm"] = bpm

        # Save as in an instance variable
        self.song_info: dict = audio_info
        return audio_info
