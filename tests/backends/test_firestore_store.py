import pytest
from datetime import datetime, timezone

from stream_checkpoint.backends.firestore_store import FirestoreCheckpointStore
from stream_checkpoint.base import Checkpoint


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeSnapshot:
    def __init__(self, data):
        self._data = data
        self.exists = data is not None

    def to_dict(self):
        return self._data


class FakeDocument:
    def __init__(self, store, doc_id):
        self._store = store
        self._doc_id = doc_id

    def set(self, data):
        self._store[self._doc_id] = data

    def get(self):
        return FakeSnapshot(self._store.get(self._doc_id))

    def delete(self):
        self._store.pop(self._doc_id, None)


class FakeCollection:
    def __init__(self, store):
        self._store = store

    def document(self, doc_id):
        return FakeDocument(self._store, doc_id)

    def stream(self):
        for doc_id, data in self._store.items():
            yield FakeSnapshot(data)


class FakeFirestoreClient:
    def __init__(self):
        self._collections = {}

    def collection(self, name):
        if name not in self._collections:
            self._collections[name] = {}
        return FakeCollection(self._collections[name])


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    return FakeFirestoreClient()


@pytest.fixture
def store(client):
    return FirestoreCheckpointStore(client, collection="checkpoints")


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe-1",
        stream_id="stream-a",
        offset=42,
        metadata={"source": "kafka"},
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestFirestoreCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.stream_id)
        assert loaded is not None
        assert loaded.pipeline_id == checkpoint.pipeline_id
        assert loaded.stream_id == checkpoint.stream_id
        assert loaded.offset == checkpoint.offset

    def test_load_returns_none_for_missing(self, store):
        result = store.load("nonexistent", "stream-x")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.pipeline_id, checkpoint.stream_id)
        assert store.load(checkpoint.pipeline_id, checkpoint.stream_id) is None

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("ghost", "stream-z")  # should not raise

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id=checkpoint.pipeline_id,
            stream_id=checkpoint.stream_id,
            offset=99,
            metadata={},
            timestamp=checkpoint.timestamp,
        )
        store.save(updated)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.stream_id)
        assert loaded.offset == 99

    def test_list_checkpoints_returns_matching(self, store):
        cp1 = Checkpoint("pipe-1", "stream-a", 1, {}, datetime.now(timezone.utc))
        cp2 = Checkpoint("pipe-1", "stream-b", 2, {}, datetime.now(timezone.utc))
        cp3 = Checkpoint("pipe-2", "stream-c", 3, {}, datetime.now(timezone.utc))
        for cp in (cp1, cp2, cp3):
            store.save(cp)
        results = store.list_checkpoints("pipe-1")
        assert len(results) == 2
        stream_ids = {r.stream_id for r in results}
        assert stream_ids == {"stream-a", "stream-b"}

    def test_list_checkpoints_empty_pipeline(self, store):
        results = store.list_checkpoints("no-such-pipeline")
        assert results == []
