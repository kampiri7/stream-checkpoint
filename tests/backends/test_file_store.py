import os
import json
import tempfile
import pytest

from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.file_store import FileCheckpointStore


@pytest.fixture
def store(tmp_path):
    """Return a FileCheckpointStore backed by a temporary directory."""
    return FileCheckpointStore(directory=str(tmp_path))


@pytest.fixture
def checkpoint():
    return Checkpoint(stream_id="stream-1", offset=42, metadata={"topic": "events"})


class TestFileCheckpointStore:
    def test_save_creates_file(self, store, checkpoint, tmp_path):
        store.save(checkpoint)
        expected_path = tmp_path / "stream-1.json"
        assert expected_path.exists()

    def test_save_file_contains_valid_json(self, store, checkpoint, tmp_path):
        store.save(checkpoint)
        with open(tmp_path / "stream-1.json") as fh:
            data = json.load(fh)
        assert data["stream_id"] == "stream-1"
        assert data["offset"] == 42

    def test_load_returns_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("stream-1")
        assert loaded is not None
        assert loaded.stream_id == checkpoint.stream_id
        assert loaded.offset == checkpoint.offset
        assert loaded.metadata == checkpoint.metadata

    def test_load_missing_returns_none(self, store):
        result = store.load("nonexistent-stream")
        assert result is None

    def test_delete_removes_file(self, store, checkpoint, tmp_path):
        store.save(checkpoint)
        store.delete("stream-1")
        assert not (tmp_path / "stream-1.json").exists()

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("ghost-stream")  # should not raise

    def test_list_streams_empty(self, store):
        assert store.list_streams() == []

    def test_list_streams_after_save(self, store):
        store.save(Checkpoint(stream_id="s1", offset=1))
        store.save(Checkpoint(stream_id="s2", offset=2))
        streams = store.list_streams()
        assert set(streams) == {"s1", "s2"}

    def test_list_streams_after_delete(self, store):
        store.save(Checkpoint(stream_id="s1", offset=1))
        store.save(Checkpoint(stream_id="s2", offset=2))
        store.delete("s1")
        assert store.list_streams() == ["s2"]

    def test_directory_created_automatically(self, tmp_path):
        nested = str(tmp_path / "nested" / "checkpoints")
        s = FileCheckpointStore(directory=nested)
        assert os.path.isdir(nested)

    def test_overwrite_existing_checkpoint(self, store):
        store.save(Checkpoint(stream_id="stream-1", offset=10))
        store.save(Checkpoint(stream_id="stream-1", offset=99))
        loaded = store.load("stream-1")
        assert loaded.offset == 99
