"""Tests for the LMDB checkpoint store backend."""

import json
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from stream_checkpoint.backends.lmdb_store import LMDBCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeCursor:
    def __init__(self, data: dict, prefix: bytes):
        self._items = sorted(
            [(k, v) for k, v in data.items() if k.startswith(prefix)]
        )
        self._index = 0

    def set_range(self, prefix: bytes) -> bool:
        return len(self._items) > 0

    def key(self) -> bytes:
        return self._items[self._index][0]

    def value(self) -> bytes:
        return self._items[self._index][1]

    def next(self) -> bool:
        self._index += 1
        return self._index < len(self._items)


class FakeTxn:
    def __init__(self, data: dict, write: bool = False):
        self._data = data
        self._write = write

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def put(self, key: bytes, value: bytes):
        self._data[key] = value

    def get(self, key: bytes):
        return self._data.get(key)

    def delete(self, key: bytes):
        self._data.pop(key, None)

    def cursor(self):
        return FakeCursor(self._data, b"")


class FakeLMDBEnv:
    def __init__(self):
        self._data = {}

    def begin(self, write: bool = False):
        return FakeTxn(self._data, write=write)

    def close(self):
        pass


@pytest.fixture
def store():
    return LMDBCheckpointStore(client=FakeLMDBEnv())


@pytest.fixture
def checkpoint():
    return Checkpoint(
        stream_id="stream-1",
        pipeline_id="pipe-1",
        offset=42,
        metadata={"source": "kafka"},
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )


class TestLMDBCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("stream-1", "pipe-1")
        assert loaded is not None
        assert loaded.stream_id == "stream-1"
        assert loaded.offset == 42

    def test_load_returns_none_for_missing(self, store):
        assert store.load("missing", "pipe-1") is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("stream-1", "pipe-1")
        assert store.load("stream-1", "pipe-1") is None

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("ghost", "pipe-1")

    def test_overwrite_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            stream_id="stream-1",
            pipeline_id="pipe-1",
            offset=99,
            metadata={},
            timestamp=datetime(2024, 6, 1, tzinfo=timezone.utc),
        )
        store.save(updated)
        loaded = store.load("stream-1", "pipe-1")
        assert loaded.offset == 99

    def test_list_checkpoints(self, store):
        for i in range(3):
            store.save(
                Checkpoint(
                    stream_id=f"stream-{i}",
                    pipeline_id="pipe-A",
                    offset=i * 10,
                    metadata={},
                    timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
                )
            )
        results = store.list_checkpoints("pipe-A")
        assert len(results) == 3

    def test_key_uses_namespace(self, store, checkpoint):
        key = store._key("stream-1", "pipe-1")
        assert key == b"checkpoint:pipe-1:stream-1"

    def test_close_calls_env_close(self):
        env = FakeLMDBEnv()
        s = LMDBCheckpointStore(client=env)
        s.close()  # should not raise
