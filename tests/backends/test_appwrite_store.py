import json
import pytest
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.appwrite_store import AppwriteCheckpointStore


class FakeDatabases:
    """Minimal fake for the Appwrite Databases service."""

    def __init__(self):
        self._store: dict[str, dict] = {}

    def create_document(self, database_id, collection_id, document_id, data):
        doc = dict(data)
        doc["$id"] = document_id
        self._store[document_id] = doc
        return doc

    def get_document(self, database_id, collection_id, document_id):
        if document_id not in self._store:
            raise KeyError(document_id)
        return self._store[document_id]

    def update_document(self, database_id, collection_id, document_id, data):
        doc = dict(data)
        doc["$id"] = document_id
        self._store[document_id] = doc
        return doc

    def delete_document(self, database_id, collection_id, document_id):
        self._store.pop(document_id, None)

    def list_documents(self, database_id, collection_id):
        return {"documents": list(self._store.values())}


@pytest.fixture
def store():
    db = FakeDatabases()
    return AppwriteCheckpointStore(
        client=db,
        database_id="db1",
        collection_id="checkpoints",
        prefix="ckpt",
    )


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        key="offset",
        value="100",
        payload={"topic": "events"},
    )


class TestAppwriteCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "offset")
        assert loaded is not None
        assert loaded.value == "100"
        assert loaded.payload == {"topic": "events"}

    def test_load_returns_none_for_missing(self, store):
        assert store.load("pipe1", "nonexistent") is None

    def test_overwrite_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id="pipe1",
            key="offset",
            value="200",
            payload={},
        )
        store.save(updated)
        loaded = store.load("pipe1", "offset")
        assert loaded.value == "200"

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "offset")
        assert store.load("pipe1", "offset") is None

    def test_delete_missing_is_noop(self, store):
        store.delete("pipe1", "ghost")  # should not raise

    def test_list_checkpoints(self, store):
        for i in range(3):
            store.save(Checkpoint(pipeline_id="pipe1", key=f"k{i}", value=str(i)))
        # Different pipeline — should not appear
        store.save(Checkpoint(pipeline_id="pipe2", key="k0", value="x"))
        results = store.list_checkpoints("pipe1")
        assert len(results) == 3
        keys = {c.key for c in results}
        assert keys == {"k0", "k1", "k2"}
