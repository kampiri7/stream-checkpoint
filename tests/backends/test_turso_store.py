"""Tests for TursoCheckpointStore."""

import json
import pytest
from stream_checkpoint.backends.turso_store import TursoCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeResult:
    def __init__(self, rows):
        self.rows = rows


class FakeTursoClient:
    """Minimal in-memory fake for libsql_client.Client."""

    def __init__(self):
        self._data = {}
        self._ddl_called = False

    def execute(self, sql, params=None):
        sql_stripped = sql.strip().upper()
        if sql_stripped.startswith("CREATE TABLE"):
            self._ddl_called = True
            return FakeResult([])
        if sql_stripped.startswith("INSERT"):
            key = (params[0], params[1])
            self._data[key] = params
            return FakeResult([])
        if sql_stripped.startswith("SELECT") and "WHERE stream_id = ? AND" in sql:
            key = (params[0], params[1])
            row = self._data.get(key)
            return FakeResult([row] if row else [])
        if sql_stripped.startswith("SELECT"):
            rows = [v for k, v in self._data.items() if k[0] == params[0]]
            return FakeResult(rows)
        if sql_stripped.startswith("DELETE"):
            key = (params[0], params[1])
            self._data.pop(key, None)
            return FakeResult([])
        return FakeResult([])


@pytest.fixture
def client():
    return FakeTursoClient()


@pytest.fixture
def store(client):
    return TursoCheckpointStore(client)


@pytest.fixture
def checkpoint():
    return Checkpoint(stream_id="stream-1", partition="0", offset="42", metadata={"key": "val"})


class TestTursoCheckpointStore:
    def test_create_table_called_on_init(self, client):
        TursoCheckpointStore(client)
        assert client._ddl_called

    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("stream-1", "0")
        assert loaded is not None
        assert loaded.stream_id == "stream-1"
        assert loaded.partition == "0"
        assert loaded.offset == "42"

    def test_load_returns_none_for_missing(self, store):
        result = store.load("no-such-stream", "0")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("stream-1", "0")
        assert store.load("stream-1", "0") is None

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("ghost", "99")  # should not raise

    def test_list_checkpoints(self, store):
        cp1 = Checkpoint(stream_id="stream-2", partition="0", offset="10", metadata={})
        cp2 = Checkpoint(stream_id="stream-2", partition="1", offset="20", metadata={})
        store.save(cp1)
        store.save(cp2)
        results = store.list_checkpoints("stream-2")
        assert len(results) == 2
        offsets = {r.offset for r in results}
        assert offsets == {"10", "20"}

    def test_list_checkpoints_empty(self, store):
        assert store.list_checkpoints("nonexistent") == []

    def test_overwrite_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(stream_id="stream-1", partition="0", offset="99", metadata={})
        store.save(updated)
        loaded = store.load("stream-1", "0")
        assert loaded.offset == "99"

    def test_metadata_preserved(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("stream-1", "0")
        assert loaded.metadata == {"key": "val"}
