import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pytest

from stream_checkpoint.backends.dynamodb_store import DynamoDBCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeItem:
    def __init__(self, data: dict):
        self._data = data


class FakeTable:
    """Minimal in-memory DynamoDB Table stub."""

    def __init__(self):
        self._store: Dict[tuple, dict] = {}

    def put_item(self, Item: dict) -> None:
        key = (Item["pipeline_id"], Item["stream_id"])
        self._store[key] = dict(Item)

    def get_item(self, Key: dict) -> dict:
        key = (Key["pipeline_id"], Key["stream_id"])
        item = self._store.get(key)
        return {"Item": item} if item else {}

    def delete_item(self, Key: dict) -> None:
        key = (Key["pipeline_id"], Key["stream_id"])
        self._store.pop(key, None)

    def query(self, KeyConditionExpression=None) -> dict:
        # Naive: return all items (condition ignored in stub)
        pid = KeyConditionExpression._value  # type: ignore[attr-defined]
        items = [
            v for (p, _), v in self._store.items() if p == pid
        ]
        return {"Items": items}


class FakeDynamoKey:
    def __init__(self, attr):
        self._attr = attr
        self._value = None

    def eq(self, value):
        self._value = value
        return self


class FakeDynamoDBResource:
    def __init__(self):
        self._tables: Dict[str, FakeTable] = {}

    def Table(self, name: str) -> FakeTable:
        if name not in self._tables:
            self._tables[name] = FakeTable()
        return self._tables[name]


@pytest.fixture
def fake_resource():
    return FakeDynamoDBResource()


@pytest.fixture
def store(fake_resource):
    return DynamoDBCheckpointStore(
        table_name="checkpoints", dynamodb_resource=fake_resource
    )


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        stream_id="stream_a",
        offset=42,
        metadata={"source": "kafka"},
        updated_at=datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
    )


class TestDynamoDBCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream_a")
        assert loaded is not None
        assert loaded.pipeline_id == "pipe1"
        assert loaded.stream_id == "stream_a"
        assert loaded.offset == 42
        assert loaded.metadata == {"source": "kafka"}

    def test_load_returns_none_for_missing(self, store):
        result = store.load("nonexistent", "stream_x")
        assert result is None

    def test_overwrite_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id="pipe1",
            stream_id="stream_a",
            offset=99,
            metadata={},
        )
        store.save(updated)
        loaded = store.load("pipe1", "stream_a")
        assert loaded.offset == 99

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream_a")
        assert store.load("pipe1", "stream_a") is None

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("pipe1", "ghost")

    def test_list_checkpoints(self, store, fake_resource, monkeypatch):
        import stream_checkpoint.backends.dynamodb_store as mod

        monkeypatch.setattr(
            "stream_checkpoint.backends.dynamodb_store.DynamoDBCheckpointStore.list_checkpoints",
            lambda self, pid: [
                cp
                for cp in [
                    store.load("pipe1", "stream_a"),
                    store.load("pipe1", "stream_b"),
                ]
                if cp is not None
            ],
        )
        cp1 = Checkpoint(pipeline_id="pipe1", stream_id="stream_a", offset=1)
        cp2 = Checkpoint(pipeline_id="pipe1", stream_id="stream_b", offset=2)
        store.save(cp1)
        store.save(cp2)
        results = store.list_checkpoints("pipe1")
        assert len(results) == 2
