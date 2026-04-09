"""Tests for base Checkpoint dataclass and BaseCheckpointStore interface."""

import pytest
from datetime import datetime
from typing import Dict, Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


# ---------------------------------------------------------------------------
# Minimal in-memory implementation used only for testing the base contract
# ---------------------------------------------------------------------------

class InMemoryStore(BaseCheckpointStore):
    def __init__(self):
        self._store: Dict[str, Checkpoint] = {}

    def save(self, checkpoint: Checkpoint) -> None:
        self._store[checkpoint.stream_id] = checkpoint

    def load(self, stream_id: str) -> Optional[Checkpoint]:
        return self._store.get(stream_id)

    def delete(self, stream_id: str) -> bool:
        if stream_id in self._store:
            del self._store[stream_id]
            return True
        return False

    def exists(self, stream_id: str) -> bool:
        return stream_id in self._store


# ---------------------------------------------------------------------------
# Checkpoint dataclass tests
# ---------------------------------------------------------------------------

class TestCheckpoint:
    def test_defaults(self):
        cp = Checkpoint(stream_id="s1", offset=42)
        assert cp.stream_id == "s1"
        assert cp.offset == 42
        assert cp.metadata == {}
        assert isinstance(cp.created_at, datetime)

    def test_round_trip_serialization(self):
        cp = Checkpoint(stream_id="s1", offset={"partition": 0, "pos": 100}, metadata={"env": "prod"})
        restored = Checkpoint.from_dict(cp.to_dict())
        assert restored.stream_id == cp.stream_id
        assert restored.offset == cp.offset
        assert restored.metadata == cp.metadata
        assert restored.created_at == cp.created_at

    def test_to_dict_keys(self):
        cp = Checkpoint(stream_id="s2", offset=0)
        d = cp.to_dict()
        assert set(d.keys()) == {"stream_id", "offset", "metadata", "created_at", "updated_at"}


# ---------------------------------------------------------------------------
# BaseCheckpointStore contract tests via InMemoryStore
# ---------------------------------------------------------------------------

class TestBaseCheckpointStore:
    @pytest.fixture()
    def store(self):
        return InMemoryStore()

    def test_save_and_load(self, store):
        cp = Checkpoint(stream_id="topic-A", offset=10)
        store.save(cp)
        loaded = store.load("topic-A")
        assert loaded is not None
        assert loaded.offset == 10

    def test_load_missing_returns_none(self, store):
        assert store.load("nonexistent") is None

    def test_exists(self, store):
        assert not store.exists("s")
        store.save(Checkpoint(stream_id="s", offset=1))
        assert store.exists("s")

    def test_delete(self, store):
        store.save(Checkpoint(stream_id="s", offset=1))
        assert store.delete("s") is True
        assert not store.exists("s")

    def test_delete_missing_returns_false(self, store):
        assert store.delete("ghost") is False

    def test_cannot_instantiate_abstract(self):
        with pytest.raises(TypeError):
            BaseCheckpointStore()  # type: ignore[abstract]
