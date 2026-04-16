import json
import pytest
from datetime import timedelta
from unittest.mock import MagicMock
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.momento_ttl_store import MomentoTTLCheckpointStore


class HitResponse:
    def __init__(self, value: str):
        self.value_string = value


class MissResponse:
    value_string = None


class FakeMomentoClient:
    def __init__(self):
        self._store = {}

    def set(self, cache_name, key, value, ttl):
        assert isinstance(ttl, timedelta)
        self._store[(cache_name, key)] = value

    def get(self, cache_name, key):
        val = self._store.get((cache_name, key))
        if val is None:
            return MissResponse()
        return HitResponse(val)

    def delete(self, cache_name, key):
        self._store.pop((cache_name, key), None)


@pytest.fixture
def client():
    return FakeMomentoClient()


@pytest.fixture
def store(client):
    return MomentoTTLCheckpointStore(client, cache_name="test-cache", prefix="cp", ttl_seconds=300)


@pytest.fixture
def checkpoint():
    return Checkpoint(stream_id="stream1", partition="0", offset="100", metadata={"source": "kafka"})


class TestMomentoTTLCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        result = store.load("stream1", "0")
        assert result is not None
        assert result.stream_id == "stream1"
        assert result.offset == "100"

    def test_load_returns_none_for_missing(self, store):
        result = store.load("nonexistent", "0")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("stream1", "0")
        assert store.load("stream1", "0") is None

    def test_key_format(self, store):
        assert store._key("s", "p") == "cp:s:p"

    def test_ttl_passed_as_timedelta(self, client, checkpoint):
        store = MomentoTTLCheckpointStore(client, "cache", ttl_seconds=600)
        store.save(checkpoint)
        # verify ttl stored correctly by checking set was called (indirectly via load)
        result = store.load(checkpoint.stream_id, checkpoint.partition)
        assert result is not None

    def test_list_checkpoints_raises(self, store):
        with pytest.raises(NotImplementedError):
            store.list_checkpoints("stream1")

    def test_overwrite_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(stream_id="stream1", partition="0", offset="200", metadata={})
        store.save(updated)
        result = store.load("stream1", "0")
        assert result.offset == "200"
