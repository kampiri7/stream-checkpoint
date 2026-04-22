"""Tests for DAXCheckpointStore."""

import json
import pytest

from stream_checkpoint.backends.dax_store import DAXCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeDAXClient:
    """Minimal in-memory fake that mimics the DAX/DynamoDB client interface."""

    def __init__(self):
        self._tables: dict = {}

    def _table(self, name: str) -> dict:
        return self._tables.setdefault(name, {})

    def put_item(self, TableName: str, Item: dict) -> None:
        key = Item["checkpoint_key"]["S"]
        self._table(TableName)[key] = Item

    def get_item(self, TableName: str, Key: dict) -> dict:
        key = Key["checkpoint_key"]["S"]
        item = self._table(TableName).get(key)
        return {"Item": item} if item else {}

    def delete_item(self, TableName: str, Key: dict) -> None:
        key = Key["checkpoint_key"]["S"]
        self._table(TableName).pop(key, None)

    def scan(self, TableName: str, FilterExpression: str, ExpressionAttributeValues: dict) -> dict:
        prefix = ExpressionAttributeValues[":pfx"]["S"]
        items = [
            item
            for k, item in self._table(TableName).items()
            if k.startswith(prefix)
        ]
        return {"Items": items}


@pytest.fixture
def client():
    return FakeDAXClient()


@pytest.fixture
def store(client):
    return DAXCheckpointStore(client, table_name="checkpoints", key_prefix="test:")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=42, metadata={"k": "v"})


class TestDAXCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded is not None
        assert loaded.pipeline_id == "pipe1"
        assert loaded.stream_id == "stream1"
        assert loaded.offset == 42

    def test_load_returns_none_for_missing(self, store):
        assert store.load("no_pipe", "no_stream") is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("ghost", "stream")

    def test_key_prefix_applied(self, client, checkpoint):
        store = DAXCheckpointStore(client, table_name="checkpoints", key_prefix="ns:")
        store.save(checkpoint)
        raw = client._table("checkpoints")
        assert any(k.startswith("ns:") for k in raw)

    def test_list_checkpoints(self, store, checkpoint):
        store.save(checkpoint)
        cp2 = Checkpoint(pipeline_id="pipe1", stream_id="stream2", offset=99)
        store.save(cp2)
        results = store.list_checkpoints("pipe1")
        stream_ids = {r.stream_id for r in results}
        assert "stream1" in stream_ids
        assert "stream2" in stream_ids

    def test_list_checkpoints_excludes_other_pipeline(self, store, checkpoint):
        store.save(checkpoint)
        other = Checkpoint(pipeline_id="other_pipe", stream_id="s1", offset=1)
        store.save(other)
        results = store.list_checkpoints("pipe1")
        assert all(r.pipeline_id == "pipe1" for r in results)

    def test_metadata_preserved(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded.metadata == {"k": "v"}
