import io
import json
import pytest

from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.minio_store import MinIOCheckpointStore


class NoSuchKey(Exception):
    def __init__(self):
        self.code = "NoSuchKey"


class FakeMinIOExceptions:
    S3Error = NoSuchKey


class FakeObject:
    def __init__(self, object_name):
        self.object_name = object_name


class FakeMinIOClient:
    def __init__(self):
        self._store = {}

    def put_object(self, bucket, key, data_stream, length, content_type=None):
        self._store[(bucket, key)] = data_stream.read()

    def get_object(self, bucket, key):
        if (bucket, key) not in self._store:
            raise NoSuchKey()
        return io.BytesIO(self._store[(bucket, key)])

    def remove_object(self, bucket, key):
        self._store.pop((bucket, key), None)

    def list_objects(self, bucket, prefix=""):
        return [
            FakeObject(k)
            for (b, k) in self._store
            if b == bucket and k.startswith(prefix)
        ]


@pytest.fixture
def client():
    return FakeMinIOClient()


@pytest.fixture
def store(client):
    return MinIOCheckpointStore(client, bucket="test-bucket", prefix="ckpts")


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        stream_id="stream1",
        offset=42,
        metadata={"topic": "events"},
    )


class TestMinIOCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded is not None
        assert loaded.pipeline_id == checkpoint.pipeline_id
        assert loaded.stream_id == checkpoint.stream_id
        assert loaded.offset == checkpoint.offset

    def test_load_returns_none_for_missing(self, store, monkeypatch):
        import minio.error as minio_error
        monkeypatch.setattr(minio_error, "S3Error", NoSuchKey)
        result = store.load("pipe1", "nonexistent")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        # After delete, the key should be gone from the fake client
        assert ("test-bucket", "ckpts/pipe1/stream1.json") not in store._client._store

    def test_key_format(self, store):
        key = store._key("my_pipe", "my_stream")
        assert key == "ckpts/my_pipe/my_stream.json"

    def test_save_stores_valid_json(self, store, checkpoint, client):
        store.save(checkpoint)
        raw = client._store[("test-bucket", "ckpts/pipe1/stream1.json")]
        parsed = json.loads(raw.decode("utf-8"))
        assert parsed["pipeline_id"] == "pipe1"
        assert parsed["offset"] == 42

    def test_list_checkpoints(self, store):
        for i in range(3):
            cp = Checkpoint(pipeline_id="pipeX", stream_id=f"s{i}", offset=i)
            store.save(cp)
        results = store.list_checkpoints("pipeX")
        assert len(results) == 3
        offsets = {r.offset for r in results}
        assert offsets == {0, 1, 2}
