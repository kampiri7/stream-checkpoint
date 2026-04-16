"""Tests for the generic KV checkpoint store."""
import json
import pytest
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.kv_store import KVCheckpointStore


class DictKVClient(dict):
    """Plain dict satisfying the required KV interface."""
    pass


@pytest.fixture
def client():
    return DictKVClient()


@pytest.fixture
def store(client):
    return KVCheckpointStore(client, prefix="ckpt:")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", partition="0", offset=42, metadata={"k": "v"})


class TestKVCheckpointStore:
    def test_save_stores_json(self, store, client, checkpoint):
        store.save(checkpoint)
        key = "ckpt:pipe1:0"
        assert key in client
        data = json.loads(client[key])
        assert data["offset"] == 42

    def test_load_returns_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "0")
        assert loaded is not None
        assert loaded.offset == 42
        assert loaded.pipeline_id == "pipe1"
        assert loaded.partition == "0"

    def test_load_returns_none_for_missing(self, store):
        assert store.load("nope", "0") is None

    def test_delete_removes_key(self, store, client, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "0")
        assert "ckpt:pipe1:0" not in client

    def test_delete_missing_is_noop(self, store):
        store.delete("ghost", "99")  # should not raise

    def test_list_checkpoints_returns_all(self, store):
        store.save(Checkpoint(pipeline_id="pipe1", partition="0", offset=1))
        store.save(Checkpoint(pipeline_id="pipe1", partition="1", offset=2))
        store.save(Checkpoint(pipeline_id="other", partition="0", offset=9))
        results = store.list_checkpoints("pipe1")
        assert len(results) == 2
        offsets = {c.offset for c in results}
        assert offsets == {1, 2}

    def test_custom_prefix(self, client):
        s = KVCheckpointStore(client, prefix="myapp::")
        s.save(Checkpoint(pipeline_id="p", partition="x", offset=7))
        assert "myapp::p:x" in client

    def test_overwrite_updates_offset(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(pipeline_id="pipe1", partition="0", offset=99)
        store.save(updated)
        loaded = store.load("pipe1", "0")
        assert loaded.offset == 99
