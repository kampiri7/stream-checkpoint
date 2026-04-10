"""Tests for the MemoryCheckpointStore backend."""

import pytest

from stream_checkpoint.backends.memory_store import MemoryCheckpointStore
from stream_checkpoint.base import Checkpoint


@pytest.fixture
def store() -> MemoryCheckpointStore:
    return MemoryCheckpointStore()


@pytest.fixture
def checkpoint() -> Checkpoint:
    return Checkpoint(
        stream_id="stream-1",
        offset=42,
        metadata={"partition": 0, "topic": "events"},
    )


class TestMemoryCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.stream_id)
        assert loaded is not None
        assert loaded.stream_id == checkpoint.stream_id
        assert loaded.offset == checkpoint.offset

    def test_load_returns_none_for_missing(self, store):
        result = store.load("nonexistent-stream")
        assert result is None

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            stream_id=checkpoint.stream_id,
            offset=100,
            metadata=checkpoint.metadata,
        )
        store.save(updated)
        loaded = store.load(checkpoint.stream_id)
        assert loaded.offset == 100

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.stream_id)
        assert store.load(checkpoint.stream_id) is None

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("nonexistent-stream")  # should not raise

    def test_exists_returns_true_after_save(self, store, checkpoint):
        store.save(checkpoint)
        assert store.exists(checkpoint.stream_id) is True

    def test_exists_returns_false_before_save(self, store, checkpoint):
        assert store.exists(checkpoint.stream_id) is False

    def test_exists_returns_false_after_delete(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.stream_id)
        assert store.exists(checkpoint.stream_id) is False

    def test_clear_removes_all_checkpoints(self, store):
        for i in range(5):
            store.save(Checkpoint(stream_id=f"stream-{i}", offset=i))
        store.clear()
        assert len(store) == 0

    def test_len_reflects_number_of_checkpoints(self, store):
        assert len(store) == 0
        store.save(Checkpoint(stream_id="s1", offset=1))
        store.save(Checkpoint(stream_id="s2", offset=2))
        assert len(store) == 2

    def test_metadata_is_preserved(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.stream_id)
        assert loaded.metadata == checkpoint.metadata
