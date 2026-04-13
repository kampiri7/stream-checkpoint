"""Tests for QdrantCheckpointStore."""

import pytest
from datetime import datetime, timezone

from stream_checkpoint.backends.qdrant_store import QdrantCheckpointStore
from stream_checkpoint.base import Checkpoint


# ---------------------------------------------------------------------------
# Fake Qdrant client
# ---------------------------------------------------------------------------

class FakeCollection:
    def __init__(self, name):
        self.name = name


class FakeCollectionsResponse:
    def __init__(self, names):
        self.collections = [FakeCollection(n) for n in names]


class FakePoint:
    def __init__(self, point_id, payload):
        self.id = point_id
        self.payload = payload


class FakeQdrantClient:
    def __init__(self, existing_collections=None):
        self._collections = set(existing_collections or [])
        self._points = {}  # collection -> {point_id: FakePoint}

    def get_collections(self):
        return FakeCollectionsResponse(list(self._collections))

    def create_collection(self, collection_name, vectors_config):
        self._collections.add(collection_name)
        self._points[collection_name] = {}

    def upsert(self, collection_name, points):
        store = self._points.setdefault(collection_name, {})
        for p in points:
            store[p.id] = p

    def scroll(self, collection_name, scroll_filter, limit, with_payload):
        store = self._points.get(collection_name, {})
        must = scroll_filter.get("must", [])
        key_filter = next(
            (f for f in must if "match" in f and "value" in f["match"]), None
        )
        matched = []
        for point in store.values():
            if key_filter:
                if point.payload.get("_ckpt_key") == key_filter["match"]["value"]:
                    matched.append(point)
            else:
                matched.append(point)
        return matched[:limit], None

    def delete(self, collection_name, points_selector):
        store = self._points.get(collection_name, {})
        filt = points_selector["filter"]["must"][0]
        key_val = filt["match"]["value"]
        to_del = [
            pid for pid, p in store.items()
            if p.payload.get("_ckpt_key") == key_val
        ]
        for pid in to_del:
            del store[pid]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    return FakeQdrantClient()


@pytest.fixture
def store(client):
    return QdrantCheckpointStore(client, collection_name="checkpoints")


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        stream_id="stream1",
        offset=42,
        metadata={"topic": "events"},
        updated_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestQdrantCheckpointStore:
    def test_collection_created_on_init(self, client):
        QdrantCheckpointStore(client, collection_name="new_col")
        names = [c.name for c in client.get_collections().collections]
        assert "new_col" in names

    def test_existing_collection_not_recreated(self, client):
        client._collections.add("checkpoints")
        client._points["checkpoints"] = {}
        store = QdrantCheckpointStore(client, collection_name="checkpoints")
        # Should still have exactly one entry
        names = [c.name for c in client.get_collections().collections]
        assert names.count("checkpoints") == 1

    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded is not None
        assert loaded.pipeline_id == "pipe1"
        assert loaded.stream_id == "stream1"
        assert loaded.offset == 42

    def test_load_returns_none_for_missing(self, store):
        result = store.load("nonexistent", "stream")
        assert result is None

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id="pipe1",
            stream_id="stream1",
            offset=99,
            metadata={},
            updated_at=datetime(2024, 6, 1, tzinfo=timezone.utc),
        )
        store.save(updated)
        loaded = store.load("pipe1", "stream1")
        assert loaded.offset == 99

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("ghost", "stream")

    def test_metadata_preserved(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded.metadata == {"topic": "events"}
