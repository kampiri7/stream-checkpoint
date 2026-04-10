from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pytest

from stream_checkpoint.backends.mongodb_store import MongoDBCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeCollection:
    """Minimal in-memory MongoDB collection stub."""

    def __init__(self):
        self._docs: Dict[Tuple[str, str], dict] = {}

    def create_index(self, keys, unique=False):
        pass  # no-op for testing

    def update_one(self, filter_: dict, update: dict, upsert: bool = False):
        key = (filter_["pipeline_id"], filter_["stream_id"])
        doc = update.get("$set", {})
        self._docs[key] = dict(doc)

    def find_one(self, filter_: dict) -> Optional[dict]:
        key = (filter_["pipeline_id"], filter_["stream_id"])
        return self._docs.get(key)

    def find(self, filter_: dict) -> List[dict]:
        pid = filter_.get("pipeline_id")
        return [v for (p, _), v in self._docs.items() if p == pid]

    def delete_one(self, filter_: dict):
        key = (filter_["pipeline_id"], filter_["stream_id"])
        self._docs.pop(key, None)


@pytest.fixture
def collection():
    return FakeCollection()


@pytest.fixture
def store(collection):
    return MongoDBCheckpointStore(collection=collection)


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        stream_id="stream_a",
        offset=10,
        metadata={"env": "prod"},
        updated_at=datetime(2024, 3, 1, 8, 0, 0, tzinfo=timezone.utc),
    )


class TestMongoDBCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream_a")
        assert loaded is not None
        assert loaded.offset == 10
        assert loaded.metadata == {"env": "prod"}

    def test_load_returns_none_for_missing(self, store):
        assert store.load("no_pipe", "no_stream") is None

    def test_overwrite_updates_offset(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id="pipe1",
            stream_id="stream_a",
            offset=777,
            metadata={},
        )
        store.save(updated)
        loaded = store.load("pipe1", "stream_a")
        assert loaded.offset == 777

    def test_delete_removes_entry(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream_a")
        assert store.load("pipe1", "stream_a") is None

    def test_delete_nonexistent_is_safe(self, store):
        store.delete("pipe1", "ghost")  # should not raise

    def test_list_checkpoints_returns_all_for_pipeline(self, store):
        cp1 = Checkpoint(pipeline_id="pipe2", stream_id="s1", offset=1)
        cp2 = Checkpoint(pipeline_id="pipe2", stream_id="s2", offset=2)
        cp3 = Checkpoint(pipeline_id="pipe3", stream_id="s1", offset=3)
        store.save(cp1)
        store.save(cp2)
        store.save(cp3)
        results = store.list_checkpoints("pipe2")
        assert len(results) == 2
        offsets = {cp.offset for cp in results}
        assert offsets == {1, 2}

    def test_list_checkpoints_empty_pipeline(self, store):
        assert store.list_checkpoints("empty_pipe") == []
