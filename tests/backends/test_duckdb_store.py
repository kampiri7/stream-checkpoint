import pytest
from unittest.mock import MagicMock, patch

from stream_checkpoint.base import Checkpoint


class FakeCursor:
    def __init__(self, rows=None):
        self._rows = rows or []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


class FakeDuckDBConnection:
    def __init__(self):
        self._data = {}

    def execute(self, sql, params=None):
        sql_stripped = sql.strip().upper()
        if sql_stripped.startswith("CREATE TABLE"):
            return FakeCursor()
        if sql_stripped.startswith("INSERT INTO"):
            key = (params[0], params[1])
            self._data[key] = params
            return FakeCursor()
        if sql_stripped.startswith("SELECT") and "WHERE" in sql_stripped:
            key = (params[0], params[1])
            row = self._data.get(key)
            return FakeCursor([row] if row else [])
        if sql_stripped.startswith("DELETE"):
            key = (params[0], params[1])
            self._data.pop(key, None)
            return FakeCursor()
        if sql_stripped.startswith("SELECT"):
            pipeline = params[0]
            rows = [v for v in self._data.values() if v[1] == pipeline]
            return FakeCursor(rows)
        return FakeCursor()

    def close(self):
        pass


@pytest.fixture
def store():
    fake_conn = FakeDuckDBConnection()
    with patch("duckdb.connect", return_value=fake_conn):
        from stream_checkpoint.backends.duckdb_store import DuckDBCheckpointStore
        s = DuckDBCheckpointStore(database=":memory:")
        return s


@pytest.fixture
def checkpoint():
    return Checkpoint(stream_id="s1", pipeline="pipe", offset="100", metadata={"k": "v"})


class TestDuckDBCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("s1", "pipe")
        assert loaded is not None
        assert loaded.stream_id == "s1"
        assert loaded.pipeline == "pipe"
        assert loaded.offset == "100"
        assert loaded.metadata == {"k": "v"}

    def test_load_returns_none_for_missing(self, store):
        result = store.load("nonexistent", "pipe")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("s1", "pipe")
        assert store.load("s1", "pipe") is None

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("ghost", "pipe")  # should not raise

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(stream_id="s1", pipeline="pipe", offset="999", metadata=None)
        store.save(updated)
        loaded = store.load("s1", "pipe")
        assert loaded.offset == "999"

    def test_list_checkpoints(self, store):
        store.save(Checkpoint(stream_id="s1", pipeline="pipe", offset="1"))
        store.save(Checkpoint(stream_id="s2", pipeline="pipe", offset="2"))
        store.save(Checkpoint(stream_id="s3", pipeline="other", offset="3"))
        results = store.list_checkpoints("pipe")
        assert len(results) == 2
        stream_ids = {r.stream_id for r in results}
        assert stream_ids == {"s1", "s2"}

    def test_close_does_not_raise(self, store):
        store.close()
