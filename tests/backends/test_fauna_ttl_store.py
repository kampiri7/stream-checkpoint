import time
import pytest
from stream_checkpoint.backends.fauna_ttl_store import FaunaTTLCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeFaunaQuery:
    def __init__(self, store):
        self._store = store

    def collection(self, name):
        return name

    def ref(self, coll, ref_id):
        return ref_id

    def create(self, coll, payload):
        data = payload["data"]
        self._store[data["record_id"]] = {"ref": data["record_id"], "data": data}

    def update(self, ref_id, payload):
        if ref_id in self._store:
            self._store[ref_id]["data"].update(payload["data"])

    def delete(self, ref_id):
        self._store.pop(ref_id, None)

    def paginate(self, match):
        return {"data": list(self._store.values())}

    def match(self, index, pipeline_id):
        return pipeline_id

    def index(self, name):
        return name


class FakeFaunaClient:
    def __init__(self):
        self._store = {}
        self.query = FakeFaunaQuery(self._store)

    def find_one(self, collection, record_id):
        return self._store.get(record_id)


@pytest.fixture
def client():
    return FakeFaunaClient()


@pytest.fixture
def store(client):
    return FaunaTTLCheckpointStore(client, collection="checkpoints")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", partition="p0", offset=42, metadata={"k": "v"})


class TestFaunaTTLCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        result = store.load("pipe1", "p0")
        assert result is not None
        assert result.offset == 42

    def test_load_missing_returns_none(self, store):
        assert store.load("pipe1", "missing") is None

    def test_delete(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "p0")
        assert store.load("pipe1", "p0") is None

    def test_ttl_expiry(self, client):
        ttl_store = FaunaTTLCheckpointStore(client, ttl_seconds=1)
        cp = Checkpoint(pipeline_id="pipe1", partition="p1", offset=10)
        ttl_store.save(cp)
        record_id = ttl_store._record_id("pipe1", "p1")
        client._store[record_id]["data"]["expires_at"] = time.time() - 1
        assert ttl_store.load("pipe1", "p1") is None

    def test_save_sets_expires_at(self, client):
        ttl_store = FaunaTTLCheckpointStore(client, ttl_seconds=300)
        cp = Checkpoint(pipeline_id="pipe2", partition="p0", offset=5)
        ttl_store.save(cp)
        record_id = ttl_store._record_id("pipe2", "p0")
        data = client._store[record_id]["data"]
        assert "expires_at" in data
        assert data["expires_at"] > time.time()

    def test_no_ttl_no_expires_at(self, store, checkpoint):
        store.save(checkpoint)
        record_id = store._record_id("pipe1", "p0")
        data = store._client._store[record_id]["data"]
        assert "expires_at" not in data
