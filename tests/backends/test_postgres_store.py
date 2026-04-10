import json
import pytest
from datetime import datetime, timezone

from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.postgres_store import PostgresCheckpointStore


# ---------------------------------------------------------------------------
# Minimal psycopg2 fakes
# ---------------------------------------------------------------------------

class FakeCursor:
    def __init__(self, store):
        self._store = store
        self._result = None

    def execute(self, sql, params=None):
        sql_stripped = " ".join(sql.split()).upper()
        if sql_stripped.startswith("CREATE TABLE"):
            return
        if sql_stripped.startswith("INSERT INTO"):
            pipeline_id, checkpoint_id, data, _ = params
            data_dict = json.loads(data) if isinstance(data, str) else data
            self._store[(pipeline_id, checkpoint_id)] = data_dict
        elif sql_stripped.startswith("SELECT DATA FROM") and "ORDER BY" in sql_stripped:
            pid = params[0]
            self._result = [
                (v,) for (p, _), v in self._store.items() if p == pid
            ]
        elif sql_stripped.startswith("SELECT DATA FROM"):
            pipeline_id, checkpoint_id = params
            val = self._store.get((pipeline_id, checkpoint_id))
            self._result = [(val,)] if val is not None else []
        elif sql_stripped.startswith("DELETE FROM"):
            pipeline_id, checkpoint_id = params
            self._store.pop((pipeline_id, checkpoint_id), None)

    def fetchone(self):
        if self._result:
            return self._result[0]
        return None

    def fetchall(self):
        return self._result or []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class FakeConnection:
    def __init__(self):
        self._store = {}

    def cursor(self):
        return FakeCursor(self._store)

    def commit(self):
        pass


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def store():
    return PostgresCheckpointStore(FakeConnection())


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe-1",
        checkpoint_id="ckpt-1",
        offset=42,
        metadata={"topic": "events"},
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPostgresCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.checkpoint_id)
        assert loaded is not None
        assert loaded.pipeline_id == checkpoint.pipeline_id
        assert loaded.checkpoint_id == checkpoint.checkpoint_id
        assert loaded.offset == checkpoint.offset

    def test_load_returns_none_for_missing(self, store):
        result = store.load("no-pipe", "no-ckpt")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.pipeline_id, checkpoint.checkpoint_id)
        assert store.load(checkpoint.pipeline_id, checkpoint.checkpoint_id) is None

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("ghost-pipe", "ghost-ckpt")  # should not raise

    def test_list_checkpoints(self, store, checkpoint):
        ckpt2 = Checkpoint(
            pipeline_id="pipe-1",
            checkpoint_id="ckpt-2",
            offset=99,
            metadata={},
            timestamp=datetime(2024, 1, 2, tzinfo=timezone.utc),
        )
        store.save(checkpoint)
        store.save(ckpt2)
        results = store.list_checkpoints("pipe-1")
        assert len(results) == 2
        ids = {r.checkpoint_id for r in results}
        assert ids == {"ckpt-1", "ckpt-2"}

    def test_list_checkpoints_empty(self, store):
        assert store.list_checkpoints("empty-pipe") == []

    def test_overwrite_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id=checkpoint.pipeline_id,
            checkpoint_id=checkpoint.checkpoint_id,
            offset=999,
            metadata={"updated": True},
            timestamp=datetime(2024, 6, 1, tzinfo=timezone.utc),
        )
        store.save(updated)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.checkpoint_id)
        assert loaded.offset == 999
