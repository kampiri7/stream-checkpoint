"""Tests for AzureCheckpointStore."""

import json
import pytest

from stream_checkpoint.backends.azure_store import AzureCheckpointStore
from stream_checkpoint.base import Checkpoint


class ResourceNotFoundError(Exception):
    """Simulates azure.core.exceptions.ResourceNotFoundError."""


class FakeBlob:
    def __init__(self, name: str, data: bytes):
        self.name = name
        self._data = data

    def readall(self) -> bytes:
        return self._data


class FakeBlobClient:
    def __init__(self, store: dict, name: str):
        self._store = store
        self._name = name

    def upload_blob(self, data: bytes, overwrite: bool = False) -> None:
        self._store[self._name] = data

    def download_blob(self) -> FakeBlob:
        if self._name not in self._store:
            raise ResourceNotFoundError(f"Blob '{self._name}' not found")
        return FakeBlob(self._name, self._store[self._name])

    def delete_blob(self) -> None:
        if self._name not in self._store:
            raise ResourceNotFoundError(f"Blob '{self._name}' not found")
        del self._store[self._name]


class FakeContainerClient:
    def __init__(self):
        self._store: dict = {}

    def get_blob_client(self, blob_name: str) -> FakeBlobClient:
        return FakeBlobClient(self._store, blob_name)

    def list_blobs(self, name_starts_with: str = ""):
        for name in list(self._store):
            if name.startswith(name_starts_with):
                yield type("BlobItem", (), {"name": name})()


@pytest.fixture
def container():
    return FakeContainerClient()


@pytest.fixture
def store(container):
    return AzureCheckpointStore(container, prefix="ckpts")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=42, metadata={"k": "v"})


class TestAzureCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.stream_id)
        assert loaded is not None
        assert loaded.offset == 42
        assert loaded.metadata == {"k": "v"}

    def test_load_returns_none_for_missing(self, store):
        result = store.load("no-pipe", "no-stream")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.pipeline_id, checkpoint.stream_id)
        assert store.load(checkpoint.pipeline_id, checkpoint.stream_id) is None

    def test_delete_missing_does_not_raise(self, store):
        store.delete("ghost-pipe", "ghost-stream")

    def test_key_uses_prefix(self, store, checkpoint, container):
        store.save(checkpoint)
        expected_key = "ckpts/pipe1/stream1.json"
        assert expected_key in container._store

    def test_blob_contains_valid_json(self, store, checkpoint, container):
        store.save(checkpoint)
        key = "ckpts/pipe1/stream1.json"
        data = json.loads(container._store[key])
        assert data["pipeline_id"] == "pipe1"
        assert data["offset"] == 42

    def test_list_checkpoints(self, store, container):
        cp1 = Checkpoint(pipeline_id="pipe1", stream_id="s1", offset=1)
        cp2 = Checkpoint(pipeline_id="pipe1", stream_id="s2", offset=2)
        cp3 = Checkpoint(pipeline_id="pipe2", stream_id="s3", offset=3)
        store.save(cp1)
        store.save(cp2)
        store.save(cp3)
        results = store.list_checkpoints("pipe1")
        stream_ids = {cp.stream_id for cp in results}
        assert stream_ids == {"s1", "s2"}

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
