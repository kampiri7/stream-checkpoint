import json
import pytest
from datetime import datetime

from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.momento_store import MomentoCheckpointStore


# ---------------------------------------------------------------------------
# Fake Momento client
# ---------------------------------------------------------------------------

class FakeMomentoClient:
    """Minimal in-memory stand-in for a Momento CacheClient."""

    class _HitResponse:
        def __init__(self, value: str):
            self.value_string = value

    class _MissResponse:
        value_string = None

    def __init__(self):
        self._store: dict = {}
        self.last_ttl = None

    def set(self, cache_name: str, key: str, value: str, ttl_seconds=None):
        self._store[(cache_name, key)] = value
        self.last_ttl = ttl_seconds

    def get(self, cache_name: str, key: str):
        value = self._store.get((cache_name, key))
        if value is None:
            return self._MissResponse()
        return self._HitResponse(value)

    def delete(self, cache_name: str, key: str):
        self._store.pop((cache_name, key), None)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    return FakeMomentoClient()


@pytest.fixture
def store(client):
    return MomentoCheckpointStore(client, cache_name="test-cache", prefix="ck:")


@pytest.fixture
def checkpoint():
    return Checkpoint(
        stream_id="orders",
        partition="0",
        offset=42,
        timestamp=datetime(2024, 6, 1, 12, 0, 0),
        metadata={"source": "momento"},
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestMomentoCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        result = store.load("orders", "0")
        assert result is not None
        assert result.stream_id == checkpoint.stream_id
        assert result.partition == checkpoint.partition
        assert result.offset == checkpoint.offset

    def test_load_returns_none_for_missing(self, store):
        assert store.load("ghost", "99") is None

    def test_delete_removes_entry(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("orders", "0")
        assert store.load("orders", "0") is None

    def test_key_uses_prefix(self, client, checkpoint):
        s = MomentoCheckpointStore(client, "c", prefix="pfx:")
        s.save(checkpoint)
        assert ("c", "pfx:orders:0") in client._store

    def test_save_with_ttl(self, client, checkpoint):
        s = MomentoCheckpointStore(client, "c", ttl=300)
        s.save(checkpoint)
        assert client.last_ttl == 300

    def test_save_without_ttl(self, client, checkpoint):
        s = MomentoCheckpointStore(client, "c", ttl=None)
        s.save(checkpoint)
        assert client.last_ttl is None

    def test_list_checkpoints_raises(self, store):
        with pytest.raises(NotImplementedError):
            store.list_checkpoints("orders")

    def test_overwrite_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            stream_id="orders",
            partition="0",
            offset=99,
            timestamp=checkpoint.timestamp,
        )
        store.save(updated)
        result = store.load("orders", "0")
        assert result.offset == 99
