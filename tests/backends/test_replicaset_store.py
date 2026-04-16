"""Tests for MongoDBReplicaSetCheckpointStore."""

import pytest
from datetime import datetime, timezone
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.replicaset_store import MongoDBReplicaSetCheckpointStore


class FakeCollection:
    def __init__(self):
        self._data = {}
        self.indexes = []

    def create_index(self, keys, unique=False):
        self.indexes.append(keys)

    def with_options(self, **kwargs):
        return self

    def _fkey(self, pipeline_id, stream_id):
        return (pipeline_id, stream_id)

    def update_one(self, filt, update, upsert=False):
        key = (filt["pipeline_id"], filt["stream_id"])
        doc = update["$set"]
        self._data[key] = dict(doc)

    def find_one(self, filt, projection=None):
        key = (filt["pipeline_id"], filt["stream_id"])
        return dict(self._data[key]) if key in self._data else None

    def delete_one(self, filt):
        key = (filt["pipeline_id"], filt["stream_id"])
        self._data.pop(key, None)

    def find(self, filt, projection=None):
        pid = filt.get("pipeline_id")
        return [
            dict(v) for (p, s), v in self._data.items() if p == pid
        ]


class FakeDatabase:
    def __init__(self):
        self._collections = {}

    def __getitem__(self, name):
        if name not in self._collections:
            self._collections[name] = FakeCollection()
        return self._collections[name]


class FakeMongoClient:
    def __init__(self):
        self._dbs = {}
        self.closed = False

    def __getitem__(self, name):
        if name not in self._dbs:
            self._dbs[name] = FakeDatabase()
        return self._dbs[name]

    def close(self):
        self.closed = True


@pytest.fixture
def client():
    return FakeMongoClient()


@pytest.fixture
def store(client):
    return MongoDBReplicaSetCheckpointStore(client)


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        stream_id="stream1",
        offset=42,
        metadata={"key": "val"},
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )


def test_save_and_load(store, checkpoint):
    store.save(checkpoint)
    loaded = store.load("pipe1", "stream1")
    assert loaded.offset == 42
    assert loaded.metadata == {"key": "val"}


def test_load_missing_returns_none(store):
    assert store.load("nope", "nope") is None


def test_delete(store, checkpoint):
    store.save(checkpoint)
    store.delete("pipe1", "stream1")
    assert store.load("pipe1", "stream1") is None


def test_list_checkpoints(store, checkpoint):
    store.save(checkpoint)
    cp2 = Checkpoint("pipe1", "stream2", 99, {}, datetime(2024, 1, 2, tzinfo=timezone.utc))
    store.save(cp2)
    results = store.list_checkpoints("pipe1")
    offsets = {c.offset for c in results}
    assert offsets == {42, 99}


def test_close(store, client):
    store.close()
    assert client.closed
