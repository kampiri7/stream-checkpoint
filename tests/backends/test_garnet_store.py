import json
import pytest

from stream_checkpoint.backends.garnet_store import GarnetCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeGarnetClient:
    """Minimal in-memory Garnet client stub."""

    def __init__(self):
        self._store = {}

    def set(self, key, value):
        self._store[key] = value

    def setex(self, key, ttl, value):
        # TTL is ignored in the fake; just store the value.
        self._store[key] = value

    def get(self, key):
        return self._store.get(key)

    def delete(self, key):
        self._store.pop(key, None)

    def scan(self, cursor, match="*", count=100):
        import fnmatch
        matched = [k for k in self._store if fnmatch.fnmatch(k, match)]
        return 0, matched


@pytest.fixture
def client():
    return FakeGarnetClient()


@pytest.fixture
def store(client):
    return GarnetCheckpointStore(client, prefix="ck:")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", checkpoint_id="ck1", offset=42, metadata={"src": "kafka"})


class TestGarnetCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.checkpoint_id)
        assert loaded is not None
        assert loaded.pipeline_id == checkpoint.pipeline_id
        assert loaded.checkpoint_id == checkpoint.checkpoint_id
        assert loaded.offset == checkpoint.offset

    def test_load_returns_none_for_missing(self, store):
        assert store.load("no-pipe", "no-ck") is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.pipeline_id, checkpoint.checkpoint_id)
        assert store.load(checkpoint.pipeline_id, checkpoint.checkpoint_id) is None

    def test_save_uses_prefix(self, client, checkpoint):
        store = GarnetCheckpointStore(client, prefix="myprefix:")
        store.save(checkpoint)
        expected_key = f"myprefix:{checkpoint.pipeline_id}:{checkpoint.checkpoint_id}"
        assert expected_key in client._store

    def test_save_with_ttl_calls_setex(self, client, checkpoint):
        store = GarnetCheckpointStore(client, prefix="ck:", ttl=300)
        store.save(checkpoint)
        key = f"ck:{checkpoint.pipeline_id}:{checkpoint.checkpoint_id}"
        assert key in client._store

    def test_list_checkpoints(self, store):
        ck1 = Checkpoint(pipeline_id="pipe2", checkpoint_id="a", offset=1)
        ck2 = Checkpoint(pipeline_id="pipe2", checkpoint_id="b", offset=2)
        store.save(ck1)
        store.save(ck2)
        results = store.list_checkpoints("pipe2")
        assert len(results) == 2
        offsets = {r.offset for r in results}
        assert offsets == {1, 2}

    def test_list_checkpoints_empty(self, store):
        assert store.list_checkpoints("ghost-pipe") == []

    def test_overwrite_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id=checkpoint.pipeline_id,
            checkpoint_id=checkpoint.checkpoint_id,
            offset=999,
        )
        store.save(updated)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.checkpoint_id)
        assert loaded.offset == 999
