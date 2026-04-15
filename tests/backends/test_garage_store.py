"""Tests for the Garage checkpoint store backend."""

import json
import pytest

from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.garage_store import GarageCheckpointStore


class NoSuchKey(Exception):
    pass


class FakeGarageExceptions:
    NoSuchKey = NoSuchKey


class FakeBody:
    def __init__(self, data: bytes):
        self._data = data

    def read(self) -> bytes:
        return self._data


class FakeGarageClient:
    def __init__(self):
        self._store: dict = {}
        self.exceptions = FakeGarageExceptions()

    def put_object(self, Bucket, Key, Body, ContentType=None):
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
    return FakeGarageClient()


@pytest.fixture
def store(client):
    return GarageCheckpointStore(client=client, bucket="test-bucket", prefix="ckpts")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=42, metadata={"k": "v"})


class TestGarageCheckpointStore:
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

    def test_key_format(self, store):
        key = store._key("pipe1", "stream1")
        assert key == "ckpts/pipe1/stream1.json"

    def test_list_checkpoints(self, store, checkpoint):
        cp2 = Checkpoint(pipeline_id="pipe1", stream_id="stream2", offset=10)
        store.save(checkpoint)
        store.save(cp2)
        results = store.list_checkpoints("pipe1")
        stream_ids = {c.stream_id for c in results}
        assert stream_ids == {"stream1", "stream2"}

    def test_list_checkpoints_empty(self, store):
        results = store.list_checkpoints("empty_pipe")
        assert results == []

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=99)
        store.save(updated)
        loaded = store.load("pipe1", "stream1")
        assert loaded.offset == 99
