import pytest
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.surrealdb_store import SurrealDBCheckpointStore


class FakeSurrealDBClient:
    """Minimal SurrealDB client stub for unit tests."""

    def __init__(self):
        self._store: dict = {}
        self._namespace = None
        self._database = None

    def use(self, namespace: str, database: str) -> None:
        self._namespace = namespace
        self._database = database

    def query(self, q: str, params: dict = None):
        params = params or {}
        q = q.strip()

        if q.startswith("UPDATE"):
            # e.g. "UPDATE table:id CONTENT $data"
            parts = q.split()
            record_id = parts[1]
            self._store[record_id] = dict(params["data"])
            return [{"status": "OK", "result": [self._store[record_id]]}]

        if q.startswith("SELECT * FROM") and "WHERE" not in q:
            parts = q.split()
            record_id = parts[3]
            if record_id in self._store:
                return [{"status": "OK", "result": [self._store[record_id]]}]
            return [{"status": "OK", "result": []}]

        if q.startswith("SELECT * FROM") and "WHERE" in q:
            pipeline_id = params.get("pid", "")
            rows = [
                v for v in self._store.values()
                if v.get("pipeline_id") == pipeline_id
            ]
            return [{"status": "OK", "result": rows}]

        if q.startswith("DELETE"):
            parts = q.split()
            record_id = parts[1]
            self._store.pop(record_id, None)
            return [{"status": "OK", "result": []}]

        return [{"status": "OK", "result": []}]


@pytest.fixture
def client():
    return FakeSurrealDBClient()


@pytest.fixture
def store(client):
    return SurrealDBCheckpointStore(client, namespace="ns", database="db", table="cp")


@pytest.fixture
def checkpoint():
    return Checkpoint(stream_id="stream1", pipeline_id="pipe1", offset=42, metadata={"k": "v"})


class TestSurrealDBCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("stream1", "pipe1")
        assert loaded is not None
        assert loaded.stream_id == "stream1"
        assert loaded.pipeline_id == "pipe1"
        assert loaded.offset == 42

    def test_load_returns_none_for_missing(self, store):
        result = store.load("nonexistent", "pipe")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("stream1", "pipe1")
        assert store.load("stream1", "pipe1") is None

    def test_overwrite_updates_offset(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(stream_id="stream1", pipeline_id="pipe1", offset=99)
        store.save(updated)
        loaded = store.load("stream1", "pipe1")
        assert loaded.offset == 99

    def test_list_checkpoints(self, store):
        store.save(Checkpoint(stream_id="s1", pipeline_id="pipe1", offset=1))
        store.save(Checkpoint(stream_id="s2", pipeline_id="pipe1", offset=2))
        store.save(Checkpoint(stream_id="s3", pipeline_id="pipe2", offset=3))
        results = store.list_checkpoints("pipe1")
        assert len(results) == 2
        offsets = {c.offset for c in results}
        assert offsets == {1, 2}

    def test_use_called_with_correct_namespace_database(self, client):
        SurrealDBCheckpointStore(client, namespace="myns", database="mydb")
        assert client._namespace == "myns"
        assert client._database == "mydb"

    def test_record_id_format(self, store):
        rid = store._record_id("stream A", "pipe B")
        assert rid == "cp:stream_A__pipe_B"
