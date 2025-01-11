import pytest
import numpy as np
from audio_analyzer.AudioAnalyzer import AudioAnalyzer


@pytest.fixture
def audio_analyzer() -> AudioAnalyzer:
    file_path: str = "../deejayssentia/music/happy_male_voice.mp3"
    return AudioAnalyzer(file_path)


def test_format_of_rhythm_data(audio_analyzer: AudioAnalyzer) -> None:
    """Test the format of rhythm data

    Args:
        audio_analyzer (AudioAnalyzer): An AudioAnalyzer instance
    """
    rhythm_data: dict = audio_analyzer.get_rhythm_data()
    assert isinstance(rhythm_data, dict)
    assert "bpm" in rhythm_data
    assert "beats" in rhythm_data
    assert "beats_confidence" in rhythm_data
    assert "beats_intervals" in rhythm_data


def test_format_of_regression_predictions(audio_analyzer: AudioAnalyzer) -> None:
    """Test the format of predictions from regression machine learning model.

    Args:
        audio_analyzer (AudioAnalyzer): An AudioAnalyzer instance
    """
    metric: tuple = audio_analyzer.calculate_prediction_metric(
        "valence_arousal")
    assert isinstance(metric, tuple)


def test_format_of_classifier_predictions(audio_analyzer: AudioAnalyzer) -> None:
    """Test the format of predictions from classifier data.

    Args:
        audio_analyzer (AudioAnalyzer): An AudioAnalyzer instance
    """
    metric: float = audio_analyzer.calculate_prediction_metric(
        "danceable_not_danceable")
    assert isinstance(metric, float)


def test_format_of_metadata(audio_analyzer: AudioAnalyzer) -> None:
    """Test the format of the metadata

    Args:
        audio_analyzer (AudioAnalyzer): An AudioAnalyzer instance
    """
    metadata: dict = audio_analyzer.get_metadata()
    assert isinstance(metadata, dict)
    assert metadata["date"] == "1955"


def test_len_of_predictions(audio_analyzer: AudioAnalyzer) -> None:
    """Test the length of predictions from individual segments of one song.

    Args:
        audio_analyzer (AudioAnalyzer): An AudioAnalyzer instance
    """
    predictions: np.ndarray = audio_analyzer.get_predictions(
        "danceable_not_danceable")
    assert isinstance(predictions, np.ndarray)
    assert len(predictions) == 187
