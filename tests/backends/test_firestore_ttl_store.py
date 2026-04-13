"""Tests for FirestoreTTLCheckpointStore."""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from stream_checkpoint.backends.firestore_ttl_store import FirestoreTTLCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeSnapshot:
    def __init__(self, exists: bool, data: dict = None):
        self.exists = exists
        self._data = data or {}

    def to_dict(self):
        return dict(self._data)


class FakeDocument:
    def __init__(self, data: dict = None):
        self._data = data

    def set(self, data):
        self._data = data

    def get(self):
        if self._data is None:
            return FakeSnapshot(exists=False)
        return FakeSnapshot(exists=True, data=self._data)

    def delete(self):
        self._data = None


class FakeCollection:
    def __init__(self):
        self._docs = {}

    def document(self, doc_id):
        if doc_id not in self._docs:
            self._docs[doc_id] = FakeDocument()
        return self._docs[doc_id]

    def where(self, field, op, value):
        fake = MagicMock()
        matching = [
            FakeSnapshot(exists=True, data=doc._data)
            for doc in self._docs.values()
            if doc._data and doc._data.get(field) == value
        ]
        fake.stream.return_value = iter(matching)
        return fake


class FakeFirestoreClient:
    def __init__(self):
        self._collections = {}

    def collection(self, name):
        if name not in self._collections:
            self._collections[name] = FakeCollection()
        return self._collections[name]


@pytest.fixture
def client():
    return FakeFirestoreClient()


@pytest.fixture
def store(client):
    return FirestoreTTLCheckpointStore(client, ttl_seconds=3600)


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        stream_id="stream1",
        offset=42,
        metadata={"key": "value"},
    )


class TestFirestoreTTLCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded is not None
        assert loaded.pipeline_id == "pipe1"
        assert loaded.stream_id == "stream1"
        assert loaded.offset == 42

    def test_load_missing_returns_none(self, store):
        assert store.load("missing", "stream") is None

    def test_delete(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None

    def test_ttl_field_written(self, client, checkpoint):
        store = FirestoreTTLCheckpointStore(client, ttl_seconds=60)
        store.save(checkpoint)
        doc_id = "pipe1__stream1"
        raw = client.collection("checkpoints").document(doc_id).get().to_dict()
        assert "expires_at" in raw

    def test_no_ttl_field_without_ttl_seconds(self, client, checkpoint):
        store = FirestoreTTLCheckpointStore(client, ttl_seconds=None)
        store.save(checkpoint)
        doc_id = "pipe1__stream1"
        raw = client.collection("checkpoints").document(doc_id).get().to_dict()
        assert "expires_at" not in raw

    def test_list_checkpoints(self, store):
        for i in range(3):
            store.save(Checkpoint(pipeline_id="pipe1", stream_id=f"s{i}", offset=i))
        store.save(Checkpoint(pipeline_id="other", stream_id="sx", offset=99))
        results = store.list_checkpoints("pipe1")
        assert len(results) == 3
        assert all(c.pipeline_id == "pipe1" for c in results)
