import pytest
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.couchdb_store import CouchDBCheckpointStore


class FakeDoc(dict):
    pass


class FakeDatabase:
    def __init__(self):
        self._docs = {}

    def __getitem__(self, doc_id):
        if doc_id not in self._docs:
            raise KeyError(doc_id)
        return dict(self._docs[doc_id])

    def __iter__(self):
        return iter(self._docs)

    def save(self, data):
        doc_id = data["_id"]
        stored = dict(data)
        stored["_rev"] = "1-abc"
        self._docs[doc_id] = stored

    def delete(self, doc):
        doc_id = doc["_id"]
        self._docs.pop(doc_id, None)


class FakeCouchDBServer:
    def __init__(self):
        self._databases = {}

    def __getitem__(self, name):
        if name not in self._databases:
            raise KeyError(name)
        return self._databases[name]

    def create(self, name):
        db = FakeDatabase()
        self._databases[name] = db
        return db


@pytest.fixture
def store():
    server = FakeCouchDBServer()
    return CouchDBCheckpointStore(client=server, database="test_checkpoints")


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        stream_id="stream1",
        offset=42,
        metadata={"key": "value"},
    )


class TestCouchDBCheckpointStore:
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

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None

    def test_delete_nonexistent_is_safe(self, store):
        store.delete("ghost", "stream")

    def test_overwrite_updates_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id="pipe1",
            stream_id="stream1",
            offset=99,
            metadata={},
        )
        store.save(updated)
        loaded = store.load("pipe1", "stream1")
        assert loaded.offset == 99

    def test_list_checkpoints(self, store):
        for i in range(3):
            cp = Checkpoint(
                pipeline_id="pipe_list",
                stream_id=f"stream{i}",
                offset=i * 10,
                metadata={},
            )
            store.save(cp)
        # save one for a different pipeline
        store.save(Checkpoint(pipeline_id="other", stream_id="s0", offset=0, metadata={}))
        results = store.list_checkpoints("pipe_list")
        assert len(results) == 3
        offsets = sorted(r.offset for r in results)
        assert offsets == [0, 10, 20]

    def test_metadata_preserved(self, store):
        cp = Checkpoint(
            pipeline_id="pipe_meta",
            stream_id="stream_meta",
            offset=7,
            metadata={"region": "us-east", "version": 2},
        )
        store.save(cp)
        loaded = store.load("pipe_meta", "stream_meta")
        assert loaded.metadata["region"] == "us-east"
        assert loaded.metadata["version"] == 2
