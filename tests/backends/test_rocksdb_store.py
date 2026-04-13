"""Tests for RocksDBCheckpointStore using a fake RocksDB client."""

import json
import pytest
from unittest.mock import MagicMock, patch

from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.rocksdb_store import RocksDBCheckpointStore


# ---------------------------------------------------------------------------
# Fake RocksDB implementation
# ---------------------------------------------------------------------------

class FakeRocksDB:
    def __init__(self):
        self._store: dict[bytes, bytes] = {}

    def put(self, key: bytes, value: bytes) -> None:
        self._store[key] = value

    def get(self, key: bytes):
        return self._store.get(key)

    def delete(self, key: bytes) -> None:
        self._store.pop(key, None)

    def itervalues(self):
        return _FakeIterValues(self._store)


class _FakeIterValues:
    def __init__(self, store):
        self._items = sorted(store.items())  # sorted by key
        self._pos = 0

    def seek(self, prefix: bytes) -> None:
        for i, (k, _) in enumerate(self._items):
            if k >= prefix:
                self._pos = i
                return
        self._pos = len(self._items)

    def __iter__(self):
        return (v for _, v in self._items[self._pos:])


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def store():
    fake_db = FakeRocksDB()
    with patch.dict("sys.modules", {"rocksdb": MagicMock()}):
        s = RocksDBCheckpointStore.__new__(RocksDBCheckpointStore)
        s._db = fake_db
        s._prefix = "checkpoint:"
    return s


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        stream_id="stream1",
        offset=42,
        metadata={"topic": "events"},
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestRocksDBCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded is not None
        assert loaded.pipeline_id == checkpoint.pipeline_id
        assert loaded.stream_id == checkpoint.stream_id
        assert loaded.offset == checkpoint.offset

    def test_load_returns_none_for_missing(self, store):
        assert store.load("no_pipe", "no_stream") is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None

    def test_delete_nonexistent_is_noop(self, store):
        store.delete("ghost", "ghost")

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id="pipe1",
            stream_id="stream1",
            offset=99,
            metadata={},
        )
        store.save(updated)
        loaded = store.load("pipe1", "stream1")
        assert loaded.offset == 99

    def test_key_format(self, store):
        key = store._key("p", "s")
        assert key == b"checkpoint:p:s"

    def test_list_checkpoints(self, store):
        for i in range(3):
            store.save(Checkpoint(pipeline_id="pipe1", stream_id=f"s{i}", offset=i))
        # Add a checkpoint for a different pipeline — should not appear.
        store.save(Checkpoint(pipeline_id="other", stream_id="sx", offset=0))
        results = store.list_checkpoints("pipe1")
        assert len(results) == 3
        assert all(c.pipeline_id == "pipe1" for c in results)
