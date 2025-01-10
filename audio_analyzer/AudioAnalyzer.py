from essentia.standard import MonoLoader, MetadataReader, TensorflowPredictEffnetDiscogs, TensorflowPredict2D, TensorflowPredictMusiCNN, RhythmExtractor2013
import numpy as np
from os.path import isfile, abspath, join
import json


class AudioAnalyzer:
    def __init__(self, file_path: str):
        abs_path = abspath(file_path)

        if isfile(abs_path):
            self.__file_path = abs_path
        else:
            raise Exception(f"Path {abs_path} does not exist.")

        self.__model_path = abspath("./audio_analyzer/models")
        self.__audio = self.__get_essentia_audio()

    @property
    def file_path(self):
        return self.__file_path

    def get_metadata(self) -> dict:
        """Get the metadata of a song

        Returns:
            dict: key and value of the metadata, e.g. {"album": "The Wildest!"}
        """
        metadata_pool = MetadataReader(filename=self.__file_path)()[7]
        metadata = {}

        try:
            for descriptor in metadata_pool.descriptorNames():
                key = descriptor.split(".")[-1]
                metadata[key] = metadata_pool[descriptor][0]
            return metadata
        except IndexError:
            print("Index out of bound")

    def get_rhythm_data(self) -> dict:
        """Get the rhythm data of a song, e.g. bpm, beats, beats_confidence, _, beats_intervals

        Returns:
            dict: rhythm data, e.g. {"bpm": 140}
        """
        rhythm_extractor = RhythmExtractor2013(method="multifeature")
        bpm, beats, beats_confidence, _, beats_intervals = rhythm_extractor(
            self.__audio)
        return {"bpm": bpm, "beats": beats, "beats_confidence": beats_confidence, "beats_intervals": beats_intervals}

    def __get_essentia_audio(self) -> MonoLoader:
        """Load the file into an Essentia audio object

        Returns:
            MonoLoader: A MonoLoader object from essentia containing the audio
        """
        return MonoLoader(filename=self.__file_path,
                          sampleRate=16000, resampleQuality=4)()

    def __get_audio_feature_config(self, audio_feature: str) -> dict:
        """Get the model name and graph filenames used for prediction of an audio feature

        Returns:
            float: _description_
        """
        config_json = abspath(
            "./audio_analyzer/data/audio_features_config.json")

        with open(config_json, "r") as file:
            parameters = json.load(file)

        return parameters[audio_feature]

    def get_predictions(self, audio_feature: str) -> np.ndarray:
        """Calculate predictions of an audio feature
            https://essentia.upf.edu/models.html for meaning of values, e.g. first column happy, second column non_happy
        Args:
            audio_feature (str): Name of the audio feature, e.g. danceability

        Returns:
            np.ndarray: Predictions for each segment of a song
        """
        audio_config = self.__get_audio_feature_config(audio_feature)
        embedding_graph_file_path = join(
            self.__model_path, audio_config["embedding_graph_filename"])
        prediction_graph_file_path = join(
            self.__model_path, audio_config["prediction_graph_filename"]
        )

        # Create embeddings based on pre-trained model (e.g. musiccn, effnet)

        if audio_config["model"] == "musicnn":
            embedding_model = TensorflowPredictMusiCNN(
                graphFilename=embedding_graph_file_path, output="model/dense/BiasAdd")
        elif audio_config["model"] == "effnet":
            embedding_model = TensorflowPredictEffnetDiscogs(
                graphFilename=embedding_graph_file_path, output="PartitionedCall:1")

        embeddings = embedding_model(self.__audio)

        # Create predictions based on algorithm (regression, classifier)

        if audio_config["algorithm"] == "regression":
            prediction_model = TensorflowPredict2D(
                graphFilename=prediction_graph_file_path, output="model/Identity")
        elif audio_config["algorithm"] == "classifier":
            prediction_model = TensorflowPredict2D(
                graphFilename=prediction_graph_file_path, output="model/Softmax")

        predictions = prediction_model(embeddings)
        return predictions

    def calculate_prediction_metric(self, audio_feature: str, category: int = 0) -> tuple | float:
        """ Calculate a single prediction metric from several predictions (for each segment in a song)
            e.g. a ratio for classifiers (e.g. danceable/non-danceable ratio)
            or a mean for regression

        Args:
            audio_feature (str): Name of the audio feature
            category (int, optional): Only relevant for classifier. 
                0 means the first category (e.g. danceable)
                1 means the second category (e.g. non-danceable)

        Returns:
            tuple | float: _description_
        """
        audio_config = self.__get_audio_feature_config(audio_feature)
        predictions = self.get_predictions(audio_feature)

        if audio_config["algorithm"] == "regression":
            avg_predictions = np.mean(predictions, axis=0)
            return tuple(avg_predictions)
        elif audio_config["algorithm"] == "classifier":
            count = np.sum(predictions[:, category] > 0.5)
            ratio = float(count / len(predictions))
            return ratio
