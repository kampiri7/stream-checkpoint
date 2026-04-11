import json
import pytest
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.tigris_store import TigrisCheckpointStore


class NoSuchKey(Exception):
    pass


class FakeTigrisExceptions:
    NoSuchKey = NoSuchKey


class FakeTigrisClient:
    def __init__(self):
        self._store = {}
        self.exceptions = FakeTigrisExceptions()

    def put_object(self, Bucket, Key, Body):
        self._store[(Bucket, Key)] = Body

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self._store:
            raise NoSuchKey(Key)

        class _Body:
            def __init__(self, data):
                self._data = data

            def read(self):
                return self._data

        return {"Body": _Body(self._store[(Bucket, Key)])}

    def delete_object(self, Bucket, Key):
        self._store.pop((Bucket, Key), None)

    def list_objects_v2(self, Bucket, Prefix):
        contents = [
            {"Key": k}
            for (b, k) in self._store
            if b == Bucket and k.startswith(Prefix)
        ]
        return {"Contents": contents}


@pytest.fixture
def client():
    return FakeTigrisClient()


@pytest.fixture
def store(client):
    return TigrisCheckpointStore(client=client, bucket="test-bucket")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=42, metadata={"k": "v"})


class TestTigrisCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded is not None
        assert loaded.offset == 42
        assert loaded.metadata == {"k": "v"}

    def test_load_returns_none_for_missing(self, store):
        assert store.load("pipe1", "missing") is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None

    def test_key_uses_prefix(self, store, checkpoint):
        store.save(checkpoint)
        key = store._key("pipe1", "stream1")
        assert key.startswith("checkpoints/")
        assert "pipe1" in key
        assert "stream1" in key

    def test_list_checkpoints(self, store):
        for i in range(3):
            store.save(Checkpoint(pipeline_id="pipe1", stream_id=f"stream{i}", offset=i))
        results = store.list_checkpoints("pipe1")
        assert len(results) == 3

    def test_list_checkpoints_empty(self, store):
        assert store.list_checkpoints("no-such-pipeline") == []

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=99)
        store.save(updated)
        loaded = store.load("pipe1", "stream1")
        assert loaded.offset == 99
