import pytest
from datetime import datetime, timezone
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.arangodb_store import ArangoDBCheckpointStore


class FakeCollection:
    def __init__(self):
        self._docs = {}

    def insert(self, doc, overwrite=False):
        self._docs[doc["_key"]] = dict(doc)

    def get(self, key):
        return dict(self._docs[key]) if key in self._docs else None

    def delete(self, key, ignore_missing=False):
        if key in self._docs:
            del self._docs[key]
        elif not ignore_missing:
            raise KeyError(key)

    def find(self, filters):
        return [
            dict(doc)
            for doc in self._docs.values()
            if all(doc.get(k) == v for k, v in filters.items())
        ]


class FakeDB:
    def __init__(self):
        self._collections = {}

    def has_collection(self, name):
        return name in self._collections

    def create_collection(self, name):
        self._collections[name] = FakeCollection()

    def collection(self, name):
        return self._collections[name]


class FakeArangoClient:
    def __init__(self):
        self._db = FakeDB()

    def db(self, name):
        return self._db


@pytest.fixture
def store():
    client = FakeArangoClient()
    return ArangoDBCheckpointStore(client, database="testdb", collection="checkpoints")


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        stream_id="stream1",
        offset=42,
        metadata={"key": "value"},
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )


class TestArangoDBCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded is not None
        assert loaded.pipeline_id == "pipe1"
        assert loaded.stream_id == "stream1"
        assert loaded.offset == 42

    def test_load_returns_none_for_missing(self, store):
        assert store.load("nope", "nope") is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None

    def test_delete_missing_does_not_raise(self, store):
        store.delete("nonexistent", "nonexistent")

    def test_exists_true_after_save(self, store, checkpoint):
        store.save(checkpoint)
        assert store.exists("pipe1", "stream1") is True

    def test_exists_false_before_save(self, store):
        assert store.exists("pipe1", "stream1") is False

    def test_list_checkpoints(self, store):
        for i in range(3):
            cp = Checkpoint(
                pipeline_id="pipe1",
                stream_id=f"stream{i}",
                offset=i,
                metadata={},
                timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
            )
            store.save(cp)
        results = store.list_checkpoints("pipe1")
        assert len(results) == 3

    def test_overwrite_existing_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id="pipe1",
            stream_id="stream1",
            offset=99,
            metadata={},
            timestamp=datetime(2024, 6, 1, tzinfo=timezone.utc),
        )
        store.save(updated)
        loaded = store.load("pipe1", "stream1")
        assert loaded.offset == 99
