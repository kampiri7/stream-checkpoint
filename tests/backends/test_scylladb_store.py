import json
import pytest

from stream_checkpoint.backends.scylladb_store import ScyllaDBCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeRow:
    def __init__(self, data):
        self.data = data


class FakeResultSet:
    def __init__(self, rows):
        self._rows = rows

    def one(self):
        return self._rows[0] if self._rows else None

    def __iter__(self):
        return iter(self._rows)


class FakeSession:
    def __init__(self):
        self._store = {}
        self.executed = []

    def execute(self, query, params=None):
        self.executed.append((query, params))
        q = query.strip().upper()

        if q.startswith("INSERT"):
            pipeline_id, stream_id, data = params
            self._store[(pipeline_id, stream_id)] = data
            return FakeResultSet([])

        if q.startswith("SELECT"):
            if "WHERE PIPELINE_ID = %S AND STREAM_ID" in q:
                pipeline_id, stream_id = params
                data = self._store.get((pipeline_id, stream_id))
                rows = [FakeRow(data)] if data else []
                return FakeResultSet(rows)
            if "WHERE PIPELINE_ID = %S" in q:
                pipeline_id = params[0]
                rows = [
                    FakeRow(v)
                    for (pid, _), v in self._store.items()
                    if pid == pipeline_id
                ]
                return FakeResultSet(rows)

        if q.startswith("DELETE"):
            pipeline_id, stream_id = params
            self._store.pop((pipeline_id, stream_id), None)
            return FakeResultSet([])

        return FakeResultSet([])


@pytest.fixture
def session():
    return FakeSession()


@pytest.fixture
def store(session):
    return ScyllaDBCheckpointStore(session=session)


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        stream_id="stream_a",
        offset=10,
        metadata={"env": "test"},
    )


class TestScyllaDBCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        result = store.load("pipe1", "stream_a")
        assert result is not None
        assert result.pipeline_id == "pipe1"
        assert result.stream_id == "stream_a"
        assert result.offset == 10

    def test_load_returns_none_for_missing(self, store):
        result = store.load("ghost", "none")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream_a")
        assert store.load("pipe1", "stream_a") is None

    def test_list_checkpoints(self, store):
        cp1 = Checkpoint(pipeline_id="pipe2", stream_id="s1", offset=1)
        cp2 = Checkpoint(pipeline_id="pipe2", stream_id="s2", offset=2)
        store.save(cp1)
        store.save(cp2)
        results = store.list_checkpoints("pipe2")
        assert len(results) == 2
        stream_ids = {r.stream_id for r in results}
        assert stream_ids == {"s1", "s2"}

    def test_list_checkpoints_empty(self, store):
        assert store.list_checkpoints("nonexistent") == []

    def test_create_table_called_on_init(self, session):
        ScyllaDBCheckpointStore(session=session)
        queries = [q.strip().upper() for q, _ in session.executed]
        assert any(q.startswith("CREATE KEYSPACE") for q in queries)
        assert any(q.startswith("CREATE TABLE") for q in queries)

    def test_overwrite_existing_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id="pipe1", stream_id="stream_a", offset=99
        )
        store.save(updated)
        result = store.load("pipe1", "stream_a")
        assert result.offset == 99
