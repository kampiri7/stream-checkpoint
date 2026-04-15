"""Tests for RedictCheckpointStore."""

from __future__ import annotations

import json
import pytest

from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.redict_store import RedictCheckpointStore


class FakeRedict:
    """Minimal in-memory Redict (Redis-compatible) fake."""

    def __init__(self):
        self._store: dict = {}

    def set(self, key: str, value: str) -> None:
        self._store[key] = value

    def setex(self, key: str, ttl: int, value: str) -> None:
        self._store[key] = value

    def get(self, key: str):
        return self._store.get(key)

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def keys(self, pattern: str) -> list:
        prefix = pattern.rstrip("*")
        return [k for k in self._store if k.startswith(prefix)]


@pytest.fixture()
def client():
    return FakeRedict()


@pytest.fixture()
def store(client):
    return RedictCheckpointStore(client, prefix="ck:")


@pytest.fixture()
def checkpoint():
    return Checkpoint(stream_id="events", partition="0", offset="42", metadata={"src": "redict"})


class TestRedictCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        result = store.load(checkpoint.stream_id, checkpoint.partition)
        assert result is not None
        assert result.offset == checkpoint.offset
        assert result.metadata == checkpoint.metadata

    def test_load_returns_none_for_missing(self, store):
        assert store.load("no-stream", "99") is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.stream_id, checkpoint.partition)
        assert store.load(checkpoint.stream_id, checkpoint.partition) is None

    def test_save_uses_prefix(self, client, checkpoint):
        store = RedictCheckpointStore(client, prefix="myprefix:")
        store.save(checkpoint)
        expected_key = f"myprefix:{checkpoint.stream_id}:{checkpoint.partition}"
        assert expected_key in client._store

    def test_save_with_ttl_calls_setex(self, client, checkpoint):
        store = RedictCheckpointStore(client, prefix="ck:", ttl=300)
        store.save(checkpoint)
        key = f"ck:{checkpoint.stream_id}:{checkpoint.partition}"
        assert key in client._store

    def test_list_checkpoints(self, store):
        cp1 = Checkpoint(stream_id="topic", partition="0", offset="1")
        cp2 = Checkpoint(stream_id="topic", partition="1", offset="2")
        cp3 = Checkpoint(stream_id="other", partition="0", offset="9")
        store.save(cp1)
        store.save(cp2)
        store.save(cp3)
        results = store.list_checkpoints("topic")
        offsets = {r.offset for r in results}
        assert offsets == {"1", "2"}

    def test_overwrite_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            stream_id=checkpoint.stream_id,
            partition=checkpoint.partition,
            offset="99",
        )
        store.save(updated)
        result = store.load(checkpoint.stream_id, checkpoint.partition)
        assert result.offset == "99"
