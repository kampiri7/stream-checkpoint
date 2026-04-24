import json
import pytest
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.cloudflare_d1_store import CloudflareD1CheckpointStore


class FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


class FakeD1Client:
    def __init__(self):
        self._store = {}
        self._tables_created = []

    def execute(self, database_id, sql, params):
        sql_lower = sql.strip().lower()
        if sql_lower.startswith("create table"):
            self._tables_created.append(sql)
            return FakeResult([])
        if sql_lower.startswith("insert"):
            key = (params[0], params[1])
            self._store[key] = params[2]
            return FakeResult([])
        if sql_lower.startswith("select") and "where pipeline=? and stream=?" in sql_lower:
            key = (params[0], params[1])
            val = self._store.get(key)
            return FakeResult([(val,)] if val is not None else [])
        if sql_lower.startswith("select") and "where pipeline=?" in sql_lower:
            rows = [(v,) for (p, _), v in self._store.items() if p == params[0]]
            return FakeResult(rows)
        if sql_lower.startswith("delete"):
            key = (params[0], params[1])
            self._store.pop(key, None)
            return FakeResult([])
        return FakeResult([])


@pytest.fixture
def client():
    return FakeD1Client()


@pytest.fixture
def store(client):
    return CloudflareD1CheckpointStore(client, database_id="db-abc123")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=42, metadata={"k": "v"})


class TestCloudflareD1CheckpointStore:
    def test_create_table_called_on_init(self, client):
        assert len(client._tables_created) == 1
        assert "checkpoints" in client._tables_created[0].lower()

    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded is not None
        assert loaded.pipeline_id == checkpoint.pipeline_id
        assert loaded.stream_id == checkpoint.stream_id
        assert loaded.offset == checkpoint.offset

    def test_load_returns_none_for_missing(self, store):
        assert store.load("no-pipe", "no-stream") is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None

    def test_list_checkpoints(self, store):
        cp1 = Checkpoint(pipeline_id="pipe2", stream_id="s1", offset=1)
        cp2 = Checkpoint(pipeline_id="pipe2", stream_id="s2", offset=2)
        store.save(cp1)
        store.save(cp2)
        results = store.list_checkpoints("pipe2")
        assert len(results) == 2
        offsets = {r.offset for r in results}
        assert offsets == {1, 2}

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=99)
        store.save(updated)
        loaded = store.load("pipe1", "stream1")
        assert loaded.offset == 99
