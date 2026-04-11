"""Tests for RedisTTLCheckpointStore."""

import pytest

from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.redis_ttl_store import RedisTTLCheckpointStore


class FakeRedis:
    """Minimal fake Redis client with SCAN support."""

    def __init__(self):
        self._data = {}
        self._ttls = {}

    def set(self, key, value):
        self._data[key] = value
        self._ttls.pop(key, None)

    def setex(self, key, ttl, value):
        self._data[key] = value
        self._ttls[key] = ttl

    def get(self, key):
        return self._data.get(key)

    def delete(self, key):
        self._data.pop(key, None)
        self._ttls.pop(key, None)

    def scan(self, cursor, match="*", count=100):
        pattern = match.rstrip("*")
        matched = [k for k in self._data if k.startswith(pattern)]
        return 0, matched


@pytest.fixture
def redis_client():
    return FakeRedis()


@pytest.fixture
def store(redis_client):
    return RedisTTLCheckpointStore(redis_client=redis_client, ttl_seconds=300)


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipeA", stream_id="streamX", offset=10)


class TestRedisTTLCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipeA", "streamX")
        assert loaded is not None
        assert loaded.offset == 10

    def test_load_missing_returns_none(self, store):
        assert store.load("ghost", "stream") is None

    def test_delete(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipeA", "streamX")
        assert store.load("pipeA", "streamX") is None

    def test_ttl_applied_on_save(self, store, checkpoint, redis_client):
        store.save(checkpoint)
        key = "ckpt:pipeA:streamX"
        assert redis_client._ttls.get(key) == 300

    def test_no_ttl_uses_plain_set(self, redis_client, checkpoint):
        store = RedisTTLCheckpointStore(redis_client=redis_client, ttl_seconds=None)
        store.save(checkpoint)
        key = "ckpt:pipeA:streamX"
        assert key not in redis_client._ttls
        assert key in redis_client._data

    def test_list_checkpoints(self, store):
        store.save(Checkpoint(pipeline_id="pipeA", stream_id="s1", offset=1))
        store.save(Checkpoint(pipeline_id="pipeA", stream_id="s2", offset=2))
        store.save(Checkpoint(pipeline_id="pipeB", stream_id="s1", offset=99))
        results = store.list_checkpoints("pipeA")
        assert len(results) == 2
        offsets = {c.offset for c in results}
        assert offsets == {1, 2}

    def test_custom_key_prefix(self, redis_client, checkpoint):
        store = RedisTTLCheckpointStore(redis_client=redis_client, key_prefix="myns")
        store.save(checkpoint)
        assert "myns:pipeA:streamX" in redis_client._data
