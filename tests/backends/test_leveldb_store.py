import json
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.leveldb_store import LevelDBCheckpointStore


class FakeIterator:
    def __init__(self, items):
        self._items = items

    def __iter__(self):
        return iter(self._items)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class FakeLevelDB:
    def __init__(self):
        self._data: dict = {}

    def put(self, key: bytes, value: bytes):
        self._data[key] = value

    def get(self, key: bytes):
        return self._data.get(key)

    def delete(self, key: bytes):
        self._data.pop(key, None)

    def iterator(self, prefix: bytes = b""):
        items = [(k, v) for k, v in self._data.items() if k.startswith(prefix)]
        return FakeIterator(items)

    def close(self):
        pass


@pytest.fixture
def store():
    fake_db = FakeLevelDB()
    with patch("stream_checkpoint.backends.leveldb_store.plyvel") as mock_plyvel:
        mock_plyvel.DB.return_value = fake_db
        s = LevelDBCheckpointStore("/fake/path", prefix="ckpt:")
        s._db = fake_db
        yield s


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        stream_id="stream1",
        offset=42,
        metadata={"source": "kafka"},
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )


class TestLevelDBCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded is not None
        assert loaded.offset == 42
        assert loaded.pipeline_id == "pipe1"
        assert loaded.stream_id == "stream1"

    def test_load_returns_none_for_missing(self, store):
        result = store.load("nonexistent", "stream")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None

    def test_list_checkpoints(self, store, checkpoint):
        store.save(checkpoint)
        cp2 = Checkpoint(
            pipeline_id="pipe1",
            stream_id="stream2",
            offset=99,
            metadata={},
            timestamp=datetime(2024, 1, 2, tzinfo=timezone.utc),
        )
        store.save(cp2)
        results = store.list_checkpoints("pipe1")
        offsets = {c.offset for c in results}
        assert offsets == {42, 99}

    def test_list_checkpoints_empty(self, store):
        assert store.list_checkpoints("no_pipeline") == []

    def test_close(self, store):
        store.close()  # Should not raise

    def test_key_format(self, store):
        key = store._key("pipeA", "streamB")
        assert key == b"ckpt:pipeA:streamB"
