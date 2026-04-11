import json
import pytest
from datetime import datetime

from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.couchbase_store import CouchbaseCheckpointStore


class FakeResult:
    def __init__(self, value):
        self.value = value


class FakeBucket:
    def __init__(self):
        self._store = {}

    def upsert(self, key, value, ttl=0):
        self._store[key] = value

    def get(self, key):
        if key not in self._store:
            raise KeyError(f"Key not found: {key}")
        return FakeResult(self._store[key])

    def remove(self, key):
        if key not in self._store:
            raise KeyError(f"Key not found: {key}")
        del self._store[key]


@pytest.fixture
def bucket():
    return FakeBucket()


@pytest.fixture
def store(bucket):
    return CouchbaseCheckpointStore(bucket)


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        stream_id="stream1",
        offset=42,
        metadata={"source": "kafka"},
    )


class TestCouchbaseCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.stream_id)
        assert loaded is not None
        assert loaded.pipeline_id == checkpoint.pipeline_id
        assert loaded.stream_id == checkpoint.stream_id
        assert loaded.offset == checkpoint.offset

    def test_load_returns_none_for_missing(self, store):
        result = store.load("no_pipe", "no_stream")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.pipeline_id, checkpoint.stream_id)
        assert store.load(checkpoint.pipeline_id, checkpoint.stream_id) is None

    def test_delete_missing_does_not_raise(self, store):
        store.delete("ghost_pipe", "ghost_stream")

    def test_key_prefix(self, bucket, checkpoint):
        store = CouchbaseCheckpointStore(bucket, key_prefix="ck:")
        store.save(checkpoint)
        expected_key = f"ck:{checkpoint.pipeline_id}:{checkpoint.stream_id}"
        assert expected_key in bucket._store

    def test_ttl_upsert_called_with_ttl(self, bucket, checkpoint):
        calls = []
        original_upsert = bucket.upsert

        def recording_upsert(key, value, ttl=0):
            calls.append(ttl)
            original_upsert(key, value, ttl=ttl)

        bucket.upsert = recording_upsert
        store = CouchbaseCheckpointStore(bucket, ttl=300)
        store.save(checkpoint)
        assert calls == [300]

    def test_list_checkpoints_raises(self, store):
        with pytest.raises(NotImplementedError):
            store.list_checkpoints("pipe1")

    def test_overwrite_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id=checkpoint.pipeline_id,
            stream_id=checkpoint.stream_id,
            offset=99,
        )
        store.save(updated)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.stream_id)
        assert loaded.offset == 99
