import json
import pytest
from datetime import datetime
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.cassandra_store import CassandraCheckpointStore


class FakeRow:
    def __init__(self, data: str):
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
            pipeline_id, stream_id, data = params[0], params[1], params[2]
            self._store[(pipeline_id, stream_id)] = data
            return FakeResultSet([])
        elif q.startswith("SELECT"):
            if "WHERE pipeline_id = %s AND stream_id = %s" in query:
                pipeline_id, stream_id = params
                row = self._store.get((pipeline_id, stream_id))
                return FakeResultSet([FakeRow(row)] if row else [])
            elif "WHERE pipeline_id = %s" in query:
                pipeline_id = params[0]
                rows = [
                    FakeRow(v)
                    for (pid, _), v in self._store.items()
                    if pid == pipeline_id
                ]
                return FakeResultSet(rows)
        elif q.startswith("DELETE"):
            pipeline_id, stream_id = params
            self._store.pop((pipeline_id, stream_id), None)
            return FakeResultSet([])
        return FakeResultSet([])


@pytest.fixture
def session():
    return FakeSession()


@pytest.fixture
def store(session):
    return CassandraCheckpointStore(session, keyspace="ks", table="cp")


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        stream_id="stream1",
        offset=42,
        metadata={"key": "value"},
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
    )


class TestCassandraCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        result = store.load("pipe1", "stream1")
        assert result is not None
        assert result.pipeline_id == checkpoint.pipeline_id
        assert result.stream_id == checkpoint.stream_id
        assert result.offset == checkpoint.offset

    def test_load_returns_none_for_missing(self, store):
        result = store.load("nonexistent", "stream")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None

    def test_list_checkpoints(self, store, checkpoint):
        store.save(checkpoint)
        cp2 = Checkpoint(
            pipeline_id="pipe1",
            stream_id="stream2",
            offset=99,
            metadata={},
            timestamp=datetime(2024, 1, 2, 0, 0, 0),
        )
        store.save(cp2)
        results = store.list_checkpoints("pipe1")
        assert len(results) == 2

    def test_save_with_ttl(self, session, checkpoint):
        store_ttl = CassandraCheckpointStore(session, keyspace="ks", table="cp", ttl=3600)
        store_ttl.save(checkpoint)
        last_query, last_params = session.executed[-1]
        assert "USING TTL" in last_query
        assert 3600 in last_params

    def test_create_table_called_on_init(self, session):
        CassandraCheckpointStore(session, keyspace="ks", table="cp")
        first_query = session.executed[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in first_query
