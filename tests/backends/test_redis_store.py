"""Tests for RedisCheckpointStore using a fake Redis client."""

import json
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

from stream_checkpoint.backends.redis_store import RedisCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeRedis:
    """Minimal in-memory Redis stub for testing."""

    def __init__(self):
        self._store: dict = {}

    def set(self, key: str, value: str) -> None:
        self._store[key] = value

    def setex(self, key: str, ttl: int, value: str) -> None:
        self._store[key] = value  # TTL not enforced in stub

    def get(self, key: str) -> Optional[str]:
        return self._store.get(key)

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def exists(self, key: str) -> int:
        return 1 if key in self._store else 0


@pytest.fixture
def store():
    fake = FakeRedis()
    return RedisCheckpointStore(prefix="test", client=fake), fake


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe-1",
        stream_id="stream-a",
        offset=42,
        metadata={"source": "kafka"},
    )


def test_save_and_load(store, checkpoint):
    s, _ = store
    s.save(checkpoint)
    loaded = s.load(checkpoint.pipeline_id, checkpoint.stream_id)
    assert loaded is not None
    assert loaded.pipeline_id == checkpoint.pipeline_id
    assert loaded.stream_id == checkpoint.stream_id
    assert loaded.offset == checkpoint.offset
    assert loaded.metadata == checkpoint.metadata


def test_load_missing_returns_none(store):
    s, _ = store
    result = s.load("no-pipe", "no-stream")
    assert result is None


def test_delete(store, checkpoint):
    s, _ = store
    s.save(checkpoint)
    s.delete(checkpoint.pipeline_id, checkpoint.stream_id)
    assert s.load(checkpoint.pipeline_id, checkpoint.stream_id) is None


def test_exists(store, checkpoint):
    s, _ = store
    assert not s.exists(checkpoint.pipeline_id, checkpoint.stream_id)
    s.save(checkpoint)
    assert s.exists(checkpoint.pipeline_id, checkpoint.stream_id)


def test_key_format(store, checkpoint):
    s, fake = store
    s.save(checkpoint)
    expected_key = "test:pipe-1:stream-a"
    assert expected_key in fake._store


def test_save_with_ttl(checkpoint):
    fake = FakeRedis()
    s = RedisCheckpointStore(prefix="test", ttl=300, client=fake)
    s.save(checkpoint)
    assert fake.get("test:pipe-1:stream-a") is not None


def test_overwrite_checkpoint(store, checkpoint):
    s, _ = store
    s.save(checkpoint)
    updated = Checkpoint(
        pipeline_id=checkpoint.pipeline_id,
        stream_id=checkpoint.stream_id,
        offset=99,
    )
    s.save(updated)
    loaded = s.load(checkpoint.pipeline_id, checkpoint.stream_id)
    assert loaded.offset == 99
