"""Tests for DynamoDBTTLCheckpointStore."""

import json
import time
import pytest

from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.dynamodb_ttl_store import DynamoDBTTLCheckpointStore


class FakeDynamoDBClient:
    """Minimal fake boto3 DynamoDB client."""

    def __init__(self):
        self._store = {}

    def put_item(self, TableName, Item):
        key = Item["checkpoint_key"]["S"]
        self._store[key] = Item

    def get_item(self, TableName, Key):
        key = Key["checkpoint_key"]["S"]
        item = self._store.get(key)
        return {"Item": item} if item else {}

    def delete_item(self, TableName, Key):
        key = Key["checkpoint_key"]["S"]
        self._store.pop(key, None)

    def scan(self, TableName, FilterExpression, ExpressionAttributeValues):
        prefix = ExpressionAttributeValues[":pfx"]["S"]
        items = [v for k, v in self._store.items() if k.startswith(prefix)]
        return {"Items": items}


@pytest.fixture
def client():
    return FakeDynamoDBClient()


@pytest.fixture
def store(client):
    return DynamoDBTTLCheckpointStore(
        table_name="checkpoints",
        dynamodb_client=client,
        ttl_seconds=3600,
    )


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=42, metadata={"key": "val"})


class TestDynamoDBTTLCheckpointStore:
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

    def test_ttl_field_is_set(self, store, checkpoint, client):
        before = int(time.time()) + 3590
        store.save(checkpoint)
        key = "checkpoint:pipe1:stream1"
        item = client._store[key]
        assert "ttl" in item
        ttl_val = int(item["ttl"]["N"])
        assert ttl_val >= before

    def test_no_ttl_when_not_configured(self, client, checkpoint):
        store_no_ttl = DynamoDBTTLCheckpointStore(
            table_name="checkpoints",
            dynamodb_client=client,
            ttl_seconds=None,
        )
        store_no_ttl.save(checkpoint)
        key = "checkpoint:pipe1:stream1"
        item = client._store[key]
        assert "ttl" not in item

    def test_list_checkpoints(self, store):
        store.save(Checkpoint(pipeline_id="pipe1", stream_id="s1", offset=1))
        store.save(Checkpoint(pipeline_id="pipe1", stream_id="s2", offset=2))
        store.save(Checkpoint(pipeline_id="pipe2", stream_id="s1", offset=99))
        results = store.list_checkpoints("pipe1")
        assert len(results) == 2
        offsets = {c.offset for c in results}
        assert offsets == {1, 2}

    def test_custom_key_prefix(self, client, checkpoint):
        store = DynamoDBTTLCheckpointStore(
            table_name="checkpoints",
            dynamodb_client=client,
            key_prefix="myapp",
        )
        store.save(checkpoint)
        assert "myapp:pipe1:stream1" in client._store
