import json
import pytest
from datetime import datetime
from stream_checkpoint.backends.tidb_store import TiDBCheckpointStore
from stream_checkpoint.base import Checkpoint


# ---------------------------------------------------------------------------
# Minimal in-memory fake that mimics a MySQL-compatible PEP 249 connection
# ---------------------------------------------------------------------------

class FakeCursor:
    def __init__(self, store):
        self._store = store
        self._result = None
        self._results = []

    def execute(self, sql, params=()):
        sql_upper = sql.strip().upper()
        if sql_upper.startswith("CREATE TABLE"):
            return
        if sql_upper.startswith("INSERT INTO"):
            pid, sid, offset, metadata, updated_at = params
            self._store[(pid, sid)] = (pid, sid, offset, metadata, updated_at)
        elif sql_upper.startswith("SELECT") and "WHERE pipeline_id" in sql.upper() and "AND stream_id" in sql.upper():
            key = (params[0], params[1])
            self._result = self._store.get(key)
        elif sql_upper.startswith("SELECT") and "WHERE pipeline_id" in sql.upper():
            pid = params[0]
            self._results = [v for k, v in self._store.items() if k[0] == pid]
        elif sql_upper.startswith("DELETE"):
            key = (params[0], params[1])
            self._store.pop(key, None)

    def fetchone(self):
        return self._result

    def fetchall(self):
        return self._results


class FakeTiDBConnection:
    def __init__(self):
        self._store = {}
        self._committed = False

    def cursor(self):
        return FakeCursor(self._store)

    def commit(self):
        self._committed = True


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def conn():
    return FakeTiDBConnection()


@pytest.fixture
def store(conn):
    return TiDBCheckpointStore(conn, table="checkpoints")


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        stream_id="stream-A",
        offset=42,
        metadata={"key": "value"},
        updated_at=datetime(2024, 1, 1, 12, 0, 0),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestTiDBCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        result = store.load("pipe1", "stream-A")
        assert result is not None
        assert result.pipeline_id == "pipe1"
        assert result.stream_id == "stream-A"
        assert result.offset == 42

    def test_load_returns_none_for_missing(self, store):
        assert store.load("no-pipe", "no-stream") is None

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id="pipe1",
            stream_id="stream-A",
            offset=99,
            metadata=None,
            updated_at=datetime(2024, 6, 1),
        )
        store.save(updated)
        result = store.load("pipe1", "stream-A")
        assert result.offset == 99

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream-A")
        assert store.load("pipe1", "stream-A") is None

    def test_delete_nonexistent_is_noop(self, store):
        store.delete("ghost", "ghost-stream")  # should not raise

    def test_load_metadata_preserved(self, store, checkpoint):
        store.save(checkpoint)
        result = store.load("pipe1", "stream-A")
        assert result.metadata == {"key": "value"}

    def test_load_null_metadata(self, store):
        cp = Checkpoint(
            pipeline_id="pipe2",
            stream_id="stream-B",
            offset=0,
            metadata=None,
            updated_at=datetime(2024, 1, 1),
        )
        store.save(cp)
        result = store.load("pipe2", "stream-B")
        assert result.metadata is None

    def test_list_checkpoints(self, store):
        for i in range(3):
            store.save(
                Checkpoint(
                    pipeline_id="pipe3",
                    stream_id=f"stream-{i}",
                    offset=i * 10,
                    metadata=None,
                    updated_at=datetime(2024, 1, 1),
                )
            )
        results = store.list_checkpoints("pipe3")
        assert len(results) == 3
        offsets = sorted(r.offset for r in results)
        assert offsets == [0, 10, 20]

    def test_list_checkpoints_empty(self, store):
        assert store.list_checkpoints("unknown-pipe") == []
