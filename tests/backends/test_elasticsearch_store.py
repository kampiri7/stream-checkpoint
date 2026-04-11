"""Tests for ElasticsearchCheckpointStore."""

import pytest
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.elasticsearch_store import ElasticsearchCheckpointStore


class FakeIndices:
    def __init__(self):
        self._existing = set()

    def exists(self, index):
        return index in self._existing

    def create(self, index, body=None):
        self._existing.add(index)


class FakeElasticsearchClient:
    def __init__(self):
        self._store = {}
        self.indices = FakeIndices()

    def index(self, index, id, body, refresh=False):
        self._store[(index, id)] = body

    def get(self, index, id):
        key = (index, id)
        if key not in self._store:
            raise KeyError(f"Document {id} not found in index {index}")
        return {"_source": self._store[key]}

    def delete(self, index, id, refresh=False):
        key = (index, id)
        if key not in self._store:
            raise KeyError(f"Document {id} not found")
        del self._store[key]

    def search(self, index, body=None):
        hits = [
            {"_source": doc}
            for (idx, _), doc in self._store.items()
            if idx == index
        ]
        return {"hits": {"hits": hits}}


@pytest.fixture
def client():
    return FakeElasticsearchClient()


@pytest.fixture
def store(client):
    return ElasticsearchCheckpointStore(client, index="test_checkpoints")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe-es-1", offset="100", metadata={"source": "kafka"})


class TestElasticsearchCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.pipeline_id)
        assert loaded is not None
        assert loaded.pipeline_id == checkpoint.pipeline_id
        assert loaded.offset == checkpoint.offset
        assert loaded.metadata == checkpoint.metadata

    def test_load_returns_none_for_missing(self, store):
        assert store.load("nonexistent") is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.pipeline_id)
        assert store.load(checkpoint.pipeline_id) is None

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("ghost-pipeline")  # should not raise

    def test_list_checkpoints_empty(self, store):
        assert store.list_checkpoints() == []

    def test_list_checkpoints_returns_all(self, store):
        cp1 = Checkpoint(pipeline_id="pipe-1", offset="10")
        cp2 = Checkpoint(pipeline_id="pipe-2", offset="20")
        store.save(cp1)
        store.save(cp2)
        checkpoints = store.list_checkpoints()
        ids = {cp.pipeline_id for cp in checkpoints}
        assert "pipe-1" in ids
        assert "pipe-2" in ids

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id=checkpoint.pipeline_id,
            offset="999",
            metadata={"updated": True},
        )
        store.save(updated)
        loaded = store.load(checkpoint.pipeline_id)
        assert loaded.offset == "999"
        assert loaded.metadata == {"updated": True}

    def test_index_created_on_init(self, client):
        store = ElasticsearchCheckpointStore(client, index="new_index")
        assert client.indices.exists("new_index")
