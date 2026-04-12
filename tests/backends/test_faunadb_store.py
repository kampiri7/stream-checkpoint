import pytest
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.faunadb_store import FaunaDBCheckpointStore


# ---------------------------------------------------------------------------
# Fake Fauna client
# ---------------------------------------------------------------------------

class FakeFaunaClient:
    """Minimal in-memory stand-in for a Fauna client."""

    def __init__(self):
        # collection_name -> {pipeline_id: {"ref": pipeline_id, "data": dict}}
        self._store = {}

    def query(self, fql):
        if "create" in fql:
            collection = fql["create"]["collection"]
            data = fql["params"]["data"]
            pid = data["pipeline_id"]
            self._store.setdefault(collection, {})[pid] = {"ref": pid, "data": data}
        elif "update" in fql:
            ref = fql["update"]
            data = fql["params"]["data"]
            collection, pid = ref
            self._store.setdefault(collection, {})[pid] = {"ref": pid, "data": data}
        elif "delete" in fql:
            ref = fql["delete"]
            collection, pid = ref
            self._store.get(collection, {}).pop(pid, None)

    def find_one(self, index, pipeline_id):
        # index encodes collection name implicitly in our fake
        for collection, docs in self._store.items():
            if pipeline_id in docs:
                return docs[pipeline_id]
        return None

    def find_ref(self, index, pipeline_id):
        for collection, docs in self._store.items():
            if pipeline_id in docs:
                return (collection, pipeline_id)
        return None

    def list_all(self, collection):
        return list(self._store.get(collection, {}).values())


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    return FakeFaunaClient()


@pytest.fixture
def store(client):
    return FaunaDBCheckpointStore(client, collection="checkpoints")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe-1", offset=42, metadata={"source": "kafka"})


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestFaunaDBCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe-1")
        assert loaded is not None
        assert loaded.pipeline_id == "pipe-1"
        assert loaded.offset == 42

    def test_load_returns_none_for_missing(self, store):
        assert store.load("nonexistent") is None

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(pipeline_id="pipe-1", offset=99, metadata={})
        store.save(updated)
        loaded = store.load("pipe-1")
        assert loaded.offset == 99

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe-1")
        assert store.load("pipe-1") is None

    def test_delete_nonexistent_is_noop(self, store):
        store.delete("ghost")  # should not raise

    def test_list_checkpoints_empty(self, store):
        assert store.list_checkpoints() == []

    def test_list_checkpoints_returns_all(self, store):
        store.save(Checkpoint(pipeline_id="pipe-1", offset=1, metadata={}))
        store.save(Checkpoint(pipeline_id="pipe-2", offset=2, metadata={}))
        items = store.list_checkpoints()
        assert len(items) == 2
        ids = {c.pipeline_id for c in items}
        assert ids == {"pipe-1", "pipe-2"}

    def test_metadata_preserved(self, store):
        cp = Checkpoint(pipeline_id="pipe-meta", offset=7, metadata={"key": "value"})
        store.save(cp)
        loaded = store.load("pipe-meta")
        assert loaded.metadata == {"key": "value"}
