import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone

from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.rethinkdb_store import RethinkDBCheckpointStore


class FakeRethinkDB:
    """Minimal stand-in for rethinkdb.RethinkDB()."""

    def __init__(self, store: dict):
        self._store = store

    def db_list(self):
        return _RunResult(["stream_checkpoint"])

    def db(self, db_name):
        return self

    def db_create(self, name):
        return _RunResult(None)

    def table_list(self):
        return _RunResult(["checkpoints"])

    def table_create(self, name, **kwargs):
        return _RunResult(None)

    def table(self, name):
        return _TableProxy(self._store)


class _RunResult:
    def __init__(self, value):
        self._value = value

    def run(self, conn):
        return self._value


class _TableProxy:
    def __init__(self, store: dict):
        self._store = store

    def insert(self, doc, conflict=None):
        self._store[doc["checkpoint_id"]] = doc
        return _RunResult(None)

    def get(self, key):
        return _RunResult(self._store.get(key))

    def filter(self, predicate: dict):
        results = [
            v for v in self._store.values()
            if all(v.get(k) == val for k, val in predicate.items())
        ]
        return _RunResult(results)

    def delete(self):
        return _RunResult(None)


@pytest.fixture
def fake_store_data():
    return {}


@pytest.fixture
def store(fake_store_data, monkeypatch):
    fake_r = FakeRethinkDB(fake_store_data)
    conn = object()
    s = RethinkDBCheckpointStore.__new__(RethinkDBCheckpointStore)
    s._conn = conn
    s._db = "stream_checkpoint"
    s._table = "checkpoints"
    s._r = fake_r
    return s


@pytest.fixture
def checkpoint():
    return Checkpoint(
        checkpoint_id="cp-1",
        pipeline_id="pipe-1",
        offset={"partition": 0, "offset": 42},
        metadata={"source": "kafka"},
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )


class TestRethinkDBCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.checkpoint_id)
        assert loaded is not None
        assert loaded.checkpoint_id == checkpoint.checkpoint_id
        assert loaded.pipeline_id == checkpoint.pipeline_id

    def test_load_returns_none_for_missing(self, store):
        assert store.load("nonexistent") is None

    def test_delete(self, store, checkpoint, fake_store_data):
        store.save(checkpoint)
        assert checkpoint.checkpoint_id in fake_store_data
        store.delete(checkpoint.checkpoint_id)
        # After delete the get proxy returns None
        assert store.load(checkpoint.checkpoint_id) is None

    def test_list_checkpoints(self, store, checkpoint):
        store.save(checkpoint)
        results = store.list_checkpoints("pipe-1")
        assert len(results) == 1
        assert results[0].checkpoint_id == "cp-1"

    def test_list_checkpoints_empty(self, store):
        results = store.list_checkpoints("no-such-pipeline")
        assert results == []
