import time
import pytest
from stream_checkpoint.backends.planetscale_ttl_store import PlanetScaleTTLCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeCursor:
    def __init__(self, store):
        self._store = store
        self._results = []

    def execute(self, sql, params=None):
        self._store._last_sql = sql
        self._store._last_params = params

    def fetchone(self):
        return self._store._fetchone_result

    def fetchall(self):
        return self._store._fetchall_result


class FakeConnection:
    def __init__(self):
        self._last_sql = None
        self._last_params = None
        self._fetchone_result = None
        self._fetchall_result = []
        self._committed = False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self._committed = True


@pytest.fixture
def conn():
    return FakeConnection()


@pytest.fixture
def store(conn):
    return PlanetScaleTTLCheckpointStore(conn, table="checkpoints", ttl=3600)


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe-1", offset={"partition": 0, "offset": 42})


class TestPlanetScaleTTLCheckpointStore:
    def test_save_commits(self, store, conn, checkpoint):
        store.save(checkpoint)
        assert conn._committed

    def test_save_passes_pipeline_id(self, store, conn, checkpoint):
        store.save(checkpoint)
        assert conn._last_params[0] == "pipe-1"

    def test_save_passes_expires_at(self, store, conn, checkpoint):
        before = time.time()
        store.save(checkpoint)
        after = time.time()
        expires_at = conn._last_params[4]
        assert before + 3600 <= expires_at <= after + 3600

    def test_load_returns_none_when_missing(self, store, conn):
        conn._fetchone_result = None
        result = store.load("missing")
        assert result is None

    def test_load_returns_checkpoint(self, store, conn):
        import json
        conn._fetchone_result = (json.dumps({"partition": 0, "offset": 42}), None, 1234567890.0)
        result = store.load("pipe-1")
        assert result is not None
        assert result.pipeline_id == "pipe-1"
        assert result.offset == {"partition": 0, "offset": 42}

    def test_load_passes_current_time(self, store, conn):
        conn._fetchone_result = None
        before = time.time()
        store.load("pipe-1")
        after = time.time()
        passed_time = conn._last_params[1]
        assert before <= passed_time <= after

    def test_delete_commits(self, store, conn):
        store.delete("pipe-1")
        assert conn._committed

    def test_delete_passes_pipeline_id(self, store, conn):
        store.delete("pipe-1")
        assert conn._last_params[0] == "pipe-1"

    def test_list_checkpoints_returns_active(self, store, conn):
        import json
        conn._fetchall_result = [
            ("pipe-1", json.dumps({"offset": 1}), None, 1234567890.0),
            ("pipe-2", json.dumps({"offset": 2}), json.dumps({"key": "val"}), 1234567891.0),
        ]
        results = store.list_checkpoints()
        assert len(results) == 2
        assert results[0].pipeline_id == "pipe-1"
        assert results[1].metadata == {"key": "val"}

    def test_list_checkpoints_empty(self, store, conn):
        conn._fetchall_result = []
        results = store.list_checkpoints()
        assert results == []
