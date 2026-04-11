"""Tests for PulsarCheckpointStore."""

import json
import pytest

from stream_checkpoint.backends.pulsar_store import PulsarCheckpointStore
from stream_checkpoint.base import Checkpoint


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeTable:
    def __init__(self):
        self._data: dict = {}

    def put(self, key: str, value: str) -> None:
        self._data[key] = value

    def get(self, key: str):
        return self._data.get(key)

    def delete(self, key: str) -> None:
        self._data.pop(key, None)

    def items(self):
        return self._data.items()


class FakePulsarClient:
    def __init__(self):
        self._tables: dict = {}

    def get_table(self, table_name: str) -> FakeTable:
        if table_name not in self._tables:
            self._tables[table_name] = FakeTable()
        return self._tables[table_name]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def store():
    return PulsarCheckpointStore(FakePulsarClient(), table_name="test_checkpoints")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=42, metadata={"key": "val"})


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPulsarCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.stream_id)
        assert loaded is not None
        assert loaded.offset == checkpoint.offset
        assert loaded.pipeline_id == checkpoint.pipeline_id
        assert loaded.stream_id == checkpoint.stream_id

    def test_load_returns_none_for_missing(self, store):
        result = store.load("nonexistent", "stream")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.pipeline_id, checkpoint.stream_id)
        assert store.load(checkpoint.pipeline_id, checkpoint.stream_id) is None

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("ghost", "stream")  # should not raise

    def test_list_checkpoints_returns_all_for_pipeline(self, store):
        c1 = Checkpoint(pipeline_id="pipeA", stream_id="s1", offset=1)
        c2 = Checkpoint(pipeline_id="pipeA", stream_id="s2", offset=2)
        c3 = Checkpoint(pipeline_id="pipeB", stream_id="s1", offset=99)
        store.save(c1)
        store.save(c2)
        store.save(c3)
        results = store.list_checkpoints("pipeA")
        assert len(results) == 2
        offsets = {r.offset for r in results}
        assert offsets == {1, 2}

    def test_list_checkpoints_empty_pipeline(self, store):
        assert store.list_checkpoints("unknown_pipe") == []

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id=checkpoint.pipeline_id,
            stream_id=checkpoint.stream_id,
            offset=999,
        )
        store.save(updated)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.stream_id)
        assert loaded.offset == 999

    def test_key_format(self, store):
        key = store._key("myPipe", "myStream")
        assert key == "ckpt:myPipe:myStream"
