import pytest
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.opensearch_store import OpenSearchCheckpointStore


class FakeIndices:
    def __init__(self):
        self._indices = set()

    def exists(self, index):
        return index in self._indices

    def create(self, index, body=None):
        self._indices.add(index)


class FakeOpenSearchClient:
    def __init__(self):
        self.indices = FakeIndices()
        self._docs = {}

    def index(self, index, id, body, pipeline=None):
        self._docs[(index, id)] = body

    def get(self, index, id):
        key = (index, id)
        if key not in self._docs:
            err = Exception("NotFoundError")
            err.__class__.__name__ = "NotFoundError"
            raise type("NotFoundError", (Exception,), {})("not found")
        return {"_source": self._docs[key]}

    def delete(self, index, id):
        key = (index, id)
        if key not in self._docs:
            raise type("NotFoundError", (Exception,), {})("not found")
        del self._docs[key]

    def search(self, index, body):
        stream_id = body["query"]["term"]["stream_id"]
        hits = [
            {"_source": doc}
            for (idx, _), doc in self._docs.items()
            if idx == index and doc.get("stream_id") == stream_id
        ]
        return {"hits": {"hits": hits}}


@pytest.fixture
def client():
    return FakeOpenSearchClient()


@pytest.fixture
def store(client):
    return OpenSearchCheckpointStore(client, index="test_checkpoints")


@pytest.fixture
def checkpoint():
    return Checkpoint(stream_id="orders", partition="0", offset="42", metadata={"key": "val"})


class TestOpenSearchCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.stream_id, checkpoint.partition)
        assert loaded is not None
        assert loaded.stream_id == checkpoint.stream_id
        assert loaded.partition == checkpoint.partition
        assert loaded.offset == checkpoint.offset

    def test_load_returns_none_for_missing(self, store):
        result = store.load("nonexistent", "0")
        assert result is None

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(stream_id=checkpoint.stream_id, partition=checkpoint.partition, offset="99")
        store.save(updated)
        loaded = store.load(checkpoint.stream_id, checkpoint.partition)
        assert loaded.offset == "99"

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.stream_id, checkpoint.partition)
        assert store.load(checkpoint.stream_id, checkpoint.partition) is None

    def test_delete_missing_does_not_raise(self, store):
        store.delete("ghost", "0")  # should not raise

    def test_list_checkpoints(self, store):
        for i in range(3):
            store.save(Checkpoint(stream_id="orders", partition=str(i), offset=str(i * 10)))
        store.save(Checkpoint(stream_id="other", partition="0", offset="1"))
        results = store.list_checkpoints("orders")
        assert len(results) == 3
        assert all(c.stream_id == "orders" for c in results)

    def test_index_created_on_init(self, client):
        store = OpenSearchCheckpointStore(client, index="new_index")
        assert client.indices.exists("new_index")

    def test_pipeline_passed_to_index(self, client, checkpoint):
        store = OpenSearchCheckpointStore(client, index="test_checkpoints", pipeline="my_pipeline")
        store.save(checkpoint)  # should not raise
