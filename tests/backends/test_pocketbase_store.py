import json
import pytest
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.pocketbase_store import PocketBaseCheckpointStore


class FakeRecord:
    def __init__(self, rec_id, doc_id, payload):
        self.id = rec_id
        self.doc_id = doc_id
        self.payload = payload


class FakeCollection:
    def __init__(self):
        self._store = {}  # doc_id -> FakeRecord
        self._counter = 0

    def get_first_list_item(self, filter_expr):
        # Parse doc_id from filter string  doc_id="<value>"
        value = filter_expr.split('"')[1]
        if value in self._store:
            return self._store[value]
        raise KeyError(f"not found: {value}")

    def create(self, data):
        self._counter += 1
        rec = FakeRecord(str(self._counter), data["doc_id"], data["payload"])
        self._store[data["doc_id"]] = rec
        return rec

    def update(self, rec_id, data):
        for doc_id, rec in self._store.items():
            if rec.id == rec_id:
                rec.payload = data["payload"]
                return rec
        raise KeyError(rec_id)

    def delete(self, rec_id):
        key = next((k for k, v in self._store.items() if v.id == rec_id), None)
        if key:
            del self._store[key]

    def get_full_list(self, query_params=None):
        prefix = query_params["filter"].split('"')[1] if query_params else ""
        return [r for r in self._store.values() if r.doc_id.startswith(prefix)]


class FakePocketBaseClient:
    def __init__(self):
        self._collections = {}

    def collection(self, name):
        if name not in self._collections:
            self._collections[name] = FakeCollection()
        return self._collections[name]


@pytest.fixture
def client():
    return FakePocketBaseClient()


@pytest.fixture
def store(client):
    return PocketBaseCheckpointStore(client, collection="checkpoints")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", source_id="src1", offset=42, metadata={"k": "v"})


class TestPocketBaseCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "src1")
        assert loaded is not None
        assert loaded.offset == 42
        assert loaded.metadata == {"k": "v"}

    def test_load_returns_none_for_missing(self, store):
        assert store.load("nope", "nope") is None

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(pipeline_id="pipe1", source_id="src1", offset=99, metadata={})
        store.save(updated)
        loaded = store.load("pipe1", "src1")
        assert loaded.offset == 99

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "src1")
        assert store.load("pipe1", "src1") is None

    def test_delete_missing_does_not_raise(self, store):
        store.delete("ghost", "ghost")  # should not raise

    def test_list_checkpoints(self, store):
        store.save(Checkpoint(pipeline_id="pipe1", source_id="src1", offset=1, metadata={}))
        store.save(Checkpoint(pipeline_id="pipe1", source_id="src2", offset=2, metadata={}))
        store.save(Checkpoint(pipeline_id="pipe2", source_id="src1", offset=3, metadata={}))
        results = store.list_checkpoints("pipe1")
        assert len(results) == 2
        offsets = {r.offset for r in results}
        assert offsets == {1, 2}

    def test_doc_id_uses_prefix(self, client):
        prefixed = PocketBaseCheckpointStore(client, prefix="myapp")
        doc_id = prefixed._doc_id("pipe", "src")
        assert doc_id.startswith("myapp")
