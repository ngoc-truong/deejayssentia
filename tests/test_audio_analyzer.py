import pytest
import numpy as np
from audio_analyzer.AudioAnalyzer import AudioAnalyzer
from pathlib import Path


@pytest.fixture
def audio_analyzer() -> AudioAnalyzer:
    file_path: Path = Path.cwd().joinpath("music", "happy_male_voice.mp3")
    return AudioAnalyzer(file_path)


def test_format_of_rhythm_data(audio_analyzer: AudioAnalyzer) -> None:
    """Test the format of rhythm data

    Args:
        audio_analyzer (AudioAnalyzer): An AudioAnalyzer instance
    """
    rhythm_data: dict = audio_analyzer.get_rhythm_data()
    assert isinstance(rhythm_data, dict)

    metadata_keys = ["bpm", "beats", "beats_confidence", "beats_intervals"]
    for key in metadata_keys:
        assert key in rhythm_data


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


@pytest.mark.skip
def test_get_all_song_information(audio_analyzer: AudioAnalyzer) -> None:
    """Test that the final song info dictionary has 20 keys

    Args:
        audio_analyzer (AudioAnalyzer): An AudioAnalyzer instance
    """
    song_info = audio_analyzer.get_complete_song_info()
    song_info_instance_var = audio_analyzer.song_info
    assert isinstance(song_info, dict) and isinstance(
        song_info_instance_var, dict)
    assert len(song_info) == 20 and len(song_info_instance_var) == 20
    assert song_info == song_info_instance_var
