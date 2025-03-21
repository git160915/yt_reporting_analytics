import json
import tempfile
from pathlib import Path
import pytest

# Import the helper function from main.py to load video IDs from a config file.
from main import load_video_ids

def test_load_video_ids_list_format():
    """Test loading a config file that is a simple JSON list."""
    with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as tmp:
        data = ["video1", "video2"]
        json.dump(data, tmp)
        tmp.flush()
        path = Path(tmp.name)
        loaded = load_video_ids(path)
        assert loaded == data

def test_load_video_ids_dict_format():
    """Test loading a config file that is a JSON object with a 'video_ids' key."""
    with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as tmp:
        data = {"video_ids": ["video3", "video4"]}
        json.dump(data, tmp)
        tmp.flush()
        path = Path(tmp.name)
        loaded = load_video_ids(path)
        assert loaded == data["video_ids"]

def test_load_video_ids_invalid_format():
    """Test that an invalid config format raises a ValueError."""
    with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as tmp:
        data = {"invalid": ["video5"]}
        json.dump(data, tmp)
        tmp.flush()
        path = Path(tmp.name)
        with pytest.raises(ValueError):
            load_video_ids(path)
