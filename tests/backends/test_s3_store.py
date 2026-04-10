import io
import json
import pytest

from stream_checkpoint.backends.s3_store import S3CheckpointStore
from stream_checkpoint.base import Checkpoint


class NoSuchKey(Exception):
    pass


class FakeS3Exceptions:
    NoSuchKey = NoSuchKey


class FakeS3Client:
    """Minimal in-memory S3 client stub."""

    def __init__(self):
        self._store: dict = {}
        self.exceptions = FakeS3Exceptions()

    def put_object(self, Bucket, Key, Body):
        self._store[(Bucket, Key)] = Body

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self._store:
            raise NoSuchKey(Key)
        return {"Body": io.BytesIO(self._store[(Bucket, Key)])}

    def delete_object(self, Bucket, Key):
        self._store.pop((Bucket, Key), None)

    def list_objects_v2(self, Bucket, Prefix):
        contents = [
            {"Key": key}
            for (b, key) in self._store
            if b == Bucket and key.startswith(Prefix)
        ]
        return {"Contents": contents}


@pytest.fixture
def s3_client():
    return FakeS3Client()


@pytest.fixture
def store(s3_client):
    return S3CheckpointStore(bucket="test-bucket", prefix="cp/", s3_client=s3_client)


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", checkpoint_id="ck1", data={"offset": 42})


class TestS3CheckpointStore:
    def test_key_format(self, store):
        assert store._key("pipe1", "ck1") == "cp/pipe1/ck1.json"

    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "ck1")
        assert loaded is not None
        assert loaded.pipeline_id == checkpoint.pipeline_id
        assert loaded.checkpoint_id == checkpoint.checkpoint_id
        assert loaded.data == checkpoint.data

    def test_load_returns_none_for_missing(self, store):
        result = store.load("pipe1", "nonexistent")
        assert result is None

    def test_save_stores_valid_json(self, store, checkpoint, s3_client):
        store.save(checkpoint)
        key = store._key("pipe1", "ck1")
        raw = s3_client._store[("test-bucket", key)]
        parsed = json.loads(raw.decode("utf-8"))
        assert parsed["pipeline_id"] == "pipe1"
        assert parsed["checkpoint_id"] == "ck1"

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "ck1")
        assert store.load("pipe1", "ck1") is None

    def test_list_checkpoints(self, store):
        cp1 = Checkpoint(pipeline_id="pipe1", checkpoint_id="ck1", data={"offset": 1})
        cp2 = Checkpoint(pipeline_id="pipe1", checkpoint_id="ck2", data={"offset": 2})
        store.save(cp1)
        store.save(cp2)
        results = store.list_checkpoints("pipe1")
        ids = {cp.checkpoint_id for cp in results}
        assert ids == {"ck1", "ck2"}

    def test_list_checkpoints_empty(self, store):
        assert store.list_checkpoints("no-such-pipeline") == []

    def test_overwrite_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(pipeline_id="pipe1", checkpoint_id="ck1", data={"offset": 99})
        store.save(updated)
        loaded = store.load("pipe1", "ck1")
        assert loaded.data["offset"] == 99
