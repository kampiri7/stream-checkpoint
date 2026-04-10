"""Tests for the GCS checkpoint store backend."""

import json
import pytest

from stream_checkpoint.backends.gcs_store import GCSCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeBlob:
    def __init__(self, name):
        self.name = name
        self._data = None
        self._deleted = False

    def upload_from_string(self, data, content_type=None):
        self._data = data

    def download_as_text(self):
        if self._data is None or self._deleted:
            raise FileNotFoundError("Blob not found")
        return self._data

    def delete(self):
        self._deleted = True
        self._data = None


class FakeBucket:
    def __init__(self):
        self._blobs = {}

    def blob(self, key):
        if key not in self._blobs:
            self._blobs[key] = FakeBlob(key)
        return self._blobs[key]


class FakeGCSClient:
    def __init__(self):
        self._buckets = {}

    def bucket(self, name):
        if name not in self._buckets:
            self._buckets[name] = FakeBucket()
        return self._buckets[name]

    def list_blobs(self, bucket_name, prefix=""):
        bucket = self._buckets.get(bucket_name, FakeBucket())
        return [
            blob
            for key, blob in bucket._blobs.items()
            if key.startswith(prefix) and blob._data is not None
        ]


@pytest.fixture
def store():
    client = FakeGCSClient()
    return GCSCheckpointStore(bucket_name="test-bucket", client=client)


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe-1",
        checkpoint_id="ckpt-1",
        offset=42,
        metadata={"source": "gcs"},
    )


class TestGCSCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.checkpoint_id)
        assert loaded is not None
        assert loaded.pipeline_id == checkpoint.pipeline_id
        assert loaded.checkpoint_id == checkpoint.checkpoint_id
        assert loaded.offset == checkpoint.offset

    def test_load_returns_none_for_missing(self, store):
        result = store.load("no-pipe", "no-ckpt")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.pipeline_id, checkpoint.checkpoint_id)
        assert store.load(checkpoint.pipeline_id, checkpoint.checkpoint_id) is None

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("ghost-pipe", "ghost-ckpt")

    def test_save_stores_valid_json(self, store, checkpoint):
        store.save(checkpoint)
        key = store._key(checkpoint.pipeline_id, checkpoint.checkpoint_id)
        blob = store._bucket._blobs[key]
        parsed = json.loads(blob._data)
        assert parsed["pipeline_id"] == checkpoint.pipeline_id
        assert parsed["offset"] == checkpoint.offset

    def test_list_checkpoints(self, store):
        for i in range(3):
            store.save(Checkpoint(pipeline_id="pipe-list", checkpoint_id=f"ckpt-{i}", offset=i))
        results = store.list_checkpoints("pipe-list")
        assert len(results) == 3
        offsets = {r.offset for r in results}
        assert offsets == {0, 1, 2}

    def test_key_format(self, store):
        key = store._key("my-pipeline", "my-checkpoint")
        assert key == "checkpoints/my-pipeline/my-checkpoint.json"

    def test_custom_prefix(self):
        client = FakeGCSClient()
        s = GCSCheckpointStore(bucket_name="b", prefix="custom/", client=client)
        key = s._key("p", "c")
        assert key.startswith("custom/")
