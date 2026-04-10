"""Tests for EtcdCheckpointStore."""

import json
import pytest

from stream_checkpoint.backends.etcd_store import EtcdCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeEtcdClient:
    """Minimal in-memory etcd client stub."""

    def __init__(self):
        self._store: dict = {}

    def put(self, key: str, value: str) -> None:
        self._store[key] = value.encode() if isinstance(value, str) else value

    def get(self, key: str):
        value = self._store.get(key)
        return value, object()  # (value, metadata)

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def get_prefix(self, prefix: str):
        for key, value in list(self._store.items()):
            if key.startswith(prefix):
                yield value, object()


@pytest.fixture
def client():
    return FakeEtcdClient()


@pytest.fixture
def store(client):
    return EtcdCheckpointStore(client, prefix="/ckpts")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=10, metadata={"env": "test"})


class TestEtcdCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.stream_id)
        assert loaded is not None
        assert loaded.offset == 10
        assert loaded.metadata == {"env": "test"}

    def test_load_returns_none_for_missing(self, store):
        assert store.load("missing-pipe", "missing-stream") is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.pipeline_id, checkpoint.stream_id)
        assert store.load(checkpoint.pipeline_id, checkpoint.stream_id) is None

    def test_delete_nonexistent_is_safe(self, store):
        store.delete("ghost", "ghost")  # should not raise

    def test_key_format(self, store, checkpoint, client):
        store.save(checkpoint)
        assert "/ckpts/pipe1/stream1" in client._store

    def test_stored_value_is_valid_json(self, store, checkpoint, client):
        store.save(checkpoint)
        raw = client._store["/ckpts/pipe1/stream1"]
        data = json.loads(raw)
        assert data["pipeline_id"] == "pipe1"
        assert data["offset"] == 10

    def test_list_checkpoints_filters_by_pipeline(self, store):
        cp1 = Checkpoint(pipeline_id="pipeA", stream_id="s1", offset=1)
        cp2 = Checkpoint(pipeline_id="pipeA", stream_id="s2", offset=2)
        cp3 = Checkpoint(pipeline_id="pipeB", stream_id="s3", offset=3)
        for cp in (cp1, cp2, cp3):
            store.save(cp)
        results = store.list_checkpoints("pipeA")
        assert len(results) == 2
        assert all(cp.pipeline_id == "pipeA" for cp in results)

    def test_overwrite_updates_offset(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id=checkpoint.pipeline_id,
            stream_id=checkpoint.stream_id,
            offset=999,
        )
        store.save(updated)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.stream_id)
        assert loaded.offset == 999
