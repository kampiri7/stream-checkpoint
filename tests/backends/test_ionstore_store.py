"""Tests for the IonStore (S3-backed Ion-compatible) checkpoint backend."""
import json
import pytest
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.ionstore_store import IonStoreCheckpointStore


class NoSuchKey(Exception):
    pass


class FakeExceptions:
    NoSuchKey = NoSuchKey


class FakeBody:
    def __init__(self, data: bytes):
        self._data = data

    def read(self) -> bytes:
        return self._data


class FakeIonClient:
    def __init__(self):
        self._store = {}
        self.exceptions = FakeExceptions()

    def put_object(self, Bucket, Key, Body):
        self._store[(Bucket, Key)] = Body

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self._store:
            raise NoSuchKey(Key)
        return {"Body": FakeBody(self._store[(Bucket, Key)])}

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
    return FakeIonClient()


@pytest.fixture
def store(client):
    return IonStoreCheckpointStore(client, bucket="test-bucket", prefix="ckpt/")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=42, metadata={"k": "v"})


class TestIonStoreCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded is not None
        assert loaded.pipeline_id == "pipe1"
        assert loaded.stream_id == "stream1"
        assert loaded.offset == 42

    def test_load_returns_none_for_missing(self, store):
        result = store.load("no_pipe", "no_stream")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None

    def test_key_format(self, store):
        key = store._key("pipeA", "streamB")
        assert key == "ckpt/pipeA/streamB.ion.json"

    def test_list_checkpoints(self, store):
        c1 = Checkpoint(pipeline_id="pipe2", stream_id="s1", offset=1)
        c2 = Checkpoint(pipeline_id="pipe2", stream_id="s2", offset=2)
        store.save(c1)
        store.save(c2)
        results = store.list_checkpoints("pipe2")
        assert len(results) == 2
        stream_ids = {r.stream_id for r in results}
        assert stream_ids == {"s1", "s2"}

    def test_overwrite_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=99)
        store.save(updated)
        loaded = store.load("pipe1", "stream1")
        assert loaded.offset == 99

    def test_metadata_preserved(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded.metadata == {"k": "v"}
