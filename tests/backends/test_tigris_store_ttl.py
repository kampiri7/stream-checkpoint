import time
import pytest
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.tigris_store_ttl import TigrisTTLCheckpointStore


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


@pytest.fixture
def client():
    return FakeTigrisClient()


@pytest.fixture
def store(client):
    return TigrisTTLCheckpointStore(client=client, bucket="ttl-bucket", ttl_seconds=60)


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=10)


class TestTigrisTTLCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded is not None
        assert loaded.offset == 10

    def test_load_missing_returns_none(self, store):
        assert store.load("pipe1", "ghost") is None

    def test_expired_checkpoint_returns_none(self, client, checkpoint):
        short_store = TigrisTTLCheckpointStore(
            client=client, bucket="ttl-bucket", ttl_seconds=-1
        )
        short_store.save(checkpoint)
        loaded = short_store.load("pipe1", "stream1")
        assert loaded is None

    def test_expired_checkpoint_is_deleted(self, client, checkpoint):
        short_store = TigrisTTLCheckpointStore(
            client=client, bucket="ttl-bucket", ttl_seconds=-1
        )
        short_store.save(checkpoint)
        short_store.load("pipe1", "stream1")
        assert short_store.load("pipe1", "stream1") is None

    def test_no_ttl_does_not_expire(self, client, checkpoint):
        no_ttl_store = TigrisTTLCheckpointStore(
            client=client, bucket="ttl-bucket", ttl_seconds=None
        )
        no_ttl_store.save(checkpoint)
        loaded = no_ttl_store.load("pipe1", "stream1")
        assert loaded is not None

    def test_delete(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None
