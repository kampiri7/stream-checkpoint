"""Tests for SpannerCheckpointStore using fakes."""

import json
import pytest
from datetime import datetime, timezone
from stream_checkpoint.backends.spanner_store import SpannerCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeKeySet:
    def __init__(self, keys=None, all_=False):
        self.keys = keys or []
        self.all_ = all_


class FakeSnapshot:
    def __init__(self, store):
        self._store = store

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def read(self, table, columns, keyset):
        key = tuple(keyset.keys[0])
        row = self._store.get(key)
        return iter([row] if row else [])

    def execute_sql(self, sql, params=None, param_types=None):
        pid = params["pid"]
        return iter(
            [r for k, r in self._store.items() if r[0] == pid]
        )


class FakeBatch:
    def __init__(self, store):
        self._store = store
        self._ops = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        for op in self._ops:
            op()

    def insert_or_update(self, table, columns, values):
        for row in values:
            key = (row[0], row[1])
            self._ops.append(lambda r=row, k=key: self._store.__setitem__(k, r))

    def delete(self, table, keyset):
        for k in keyset.keys:
            self._ops.append(lambda key=tuple(k): self._store.pop(key, None))


class FakeDatabase:
    def __init__(self):
        self._rows = {}

    def update_ddl(self, ddl):
        class Op:
            def result(self):
                pass
        return Op()

    def snapshot(self):
        return FakeSnapshot(self._rows)

    def batch(self):
        return FakeBatch(self._rows)


class FakeInstance:
    def __init__(self):
        self._db = FakeDatabase()

    def database(self, database_id):
        return self._db


class FakeSpannerClient:
    def __init__(self):
        self._instance = FakeInstance()

    def instance(self, instance_id):
        return self._instance


@pytest.fixture
def store():
    s = SpannerCheckpointStore.__new__(SpannerCheckpointStore)
    fake_client = FakeSpannerClient()
    s._instance = fake_client.instance("test-instance")
    s._database = s._instance.database("test-db")
    s._table = "checkpoints"
    return s


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        stream_id="stream1",
        offset="100",
        metadata={"key": "value"},
        updated_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )


class TestSpannerCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded is not None
        assert loaded.pipeline_id == "pipe1"
        assert loaded.stream_id == "stream1"
        assert loaded.offset == "100"
        assert loaded.metadata == {"key": "value"}

    def test_load_returns_none_for_missing(self, store):
        result = store.load("nonexistent", "stream")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id="pipe1",
            stream_id="stream1",
            offset="999",
            metadata={},
            updated_at=datetime(2024, 6, 1, tzinfo=timezone.utc),
        )
        store.save(updated)
        loaded = store.load("pipe1", "stream1")
        assert loaded.offset == "999"

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("pipe1", "stream_missing")
