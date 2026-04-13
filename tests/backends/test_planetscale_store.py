import json
import pytest
from stream_checkpoint.backends.planetscale_store import PlanetScaleCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeCursor:
    def __init__(self, store):
        self._store = store
        self._results = []

    def execute(self, sql, params=None):
        sql_stripped = sql.strip()
        if sql_stripped.startswith("USE") or sql_stripped.startswith("CREATE TABLE"):
            return
        if sql_stripped.startswith("INSERT INTO"):
            stream_id, partition, offset, meta = params
            key = (stream_id, partition)
            self._store[key] = (stream_id, partition, offset, meta)
        elif sql_stripped.startswith("SELECT") and "WHERE `stream_id` = %s AND" in sql_stripped:
            stream_id, partition = params
            key = (stream_id, partition)
            self._results = [self._store[key]] if key in self._store else []
        elif sql_stripped.startswith("SELECT") and "WHERE `stream_id` = %s" in sql_stripped:
            (stream_id,) = params
            self._results = [v for k, v in self._store.items() if k[0] == stream_id]
        elif sql_stripped.startswith("DELETE"):
            stream_id, partition = params
            key = (stream_id, partition)
            self._store.pop(key, None)

    def fetchone(self):
        return self._results[0] if self._results else None

    def fetchall(self):
        return self._results


class FakePlanetScaleConnection:
    def __init__(self):
        self._store = {}
        self._committed = False

    def cursor(self):
        return FakeCursor(self._store)

    def commit(self):
        self._committed = True


@pytest.fixture
def conn():
    return FakePlanetScaleConnection()


@pytest.fixture
def store(conn):
    return PlanetScaleCheckpointStore(client=conn, database="testdb", table="checkpoints")


@pytest.fixture
def checkpoint():
    return Checkpoint(stream_id="orders", partition="p0", offset=42, metadata={"env": "test"})


class TestPlanetScaleCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("orders", "p0")
        assert loaded is not None
        assert loaded.stream_id == "orders"
        assert loaded.partition == "p0"
        assert loaded.offset == 42
        assert loaded.metadata == {"env": "test"}

    def test_load_returns_none_for_missing(self, store):
        result = store.load("nonexistent", "p0")
        assert result is None

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(stream_id="orders", partition="p0", offset=99, metadata={})
        store.save(updated)
        loaded = store.load("orders", "p0")
        assert loaded.offset == 99

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("orders", "p0")
        result = store.load("orders", "p0")
        assert result is None

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("ghost", "p0")  # should not raise

    def test_list_checkpoints(self, store):
        store.save(Checkpoint(stream_id="events", partition="p0", offset=1))
        store.save(Checkpoint(stream_id="events", partition="p1", offset=2))
        store.save(Checkpoint(stream_id="other", partition="p0", offset=3))
        results = store.list_checkpoints("events")
        assert len(results) == 2
        partitions = {c.partition for c in results}
        assert partitions == {"p0", "p1"}

    def test_list_checkpoints_empty(self, store):
        results = store.list_checkpoints("nope")
        assert results == []

    def test_metadata_none_handled(self, store):
        cp = Checkpoint(stream_id="s", partition="p", offset=0, metadata=None)
        store.save(cp)
        loaded = store.load("s", "p")
        assert loaded.metadata == {}
