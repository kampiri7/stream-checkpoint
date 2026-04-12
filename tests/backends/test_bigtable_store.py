"""Tests for BigtableCheckpointStore."""

import json
import pytest
from datetime import datetime, timezone

from stream_checkpoint.backends.bigtable_store import BigtableCheckpointStore
from stream_checkpoint.base import Checkpoint


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeCell:
    def __init__(self, value: bytes):
        self.value = value


class FakeDirectRow:
    def __init__(self, row_key: bytes, store: dict):
        self._row_key = row_key
        self._store = store
        self._pending: bytes | None = None
        self._delete = False

    def set_cell(self, cf, col, value):
        self._pending = value

    def delete(self):
        self._delete = True

    def commit(self):
        if self._delete:
            self._store.pop(self._row_key, None)
        elif self._pending is not None:
            self._store[self._row_key] = self._pending


class FakeRow:
    def __init__(self, row_key: bytes, value: bytes, cf: str, col: bytes):
        self.cells = {cf: {col: [FakeCell(value)]}}


class FakeTable:
    def __init__(self):
        self._data: dict[bytes, bytes] = {}
        self._cf = "cf1"
        self._col = b"data"

    def direct_row(self, row_key: bytes) -> FakeDirectRow:
        return FakeDirectRow(row_key, self._data)

    def read_row(self, row_key: bytes):
        value = self._data.get(row_key)
        if value is None:
            return None
        return FakeRow(row_key, value, self._cf, self._col)

    def read_rows(self, start_key: bytes, end_key: bytes):
        for key, value in self._data.items():
            if start_key <= key < end_key:
                yield FakeRow(key, value, self._cf, self._col)


class FakeInstance:
    def __init__(self):
        self._tables: dict[str, FakeTable] = {}

    def table(self, table_id: str) -> FakeTable:
        if table_id not in self._tables:
            self._tables[table_id] = FakeTable()
        return self._tables[table_id]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def store():
    instance = FakeInstance()
    return BigtableCheckpointStore(instance, table_id="checkpoints", prefix="test:")


@pytest.fixture
def checkpoint():
    return Checkpoint(
        stream_id="orders",
        partition="0",
        offset="42",
        metadata={"source": "kafka"},
        timestamp=datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestBigtableCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.stream_id, checkpoint.partition)
        assert loaded is not None
        assert loaded.stream_id == checkpoint.stream_id
        assert loaded.partition == checkpoint.partition
        assert loaded.offset == checkpoint.offset

    def test_load_returns_none_for_missing(self, store):
        result = store.load("nonexistent", "99")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.stream_id, checkpoint.partition)
        assert store.load(checkpoint.stream_id, checkpoint.partition) is None

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("ghost", "0")  # should not raise

    def test_list_checkpoints_returns_all_partitions(self, store):
        cp1 = Checkpoint(stream_id="events", partition="0", offset="10", metadata={})
        cp2 = Checkpoint(stream_id="events", partition="1", offset="20", metadata={})
        cp_other = Checkpoint(stream_id="other", partition="0", offset="5", metadata={})
        store.save(cp1)
        store.save(cp2)
        store.save(cp_other)
        results = store.list_checkpoints("events")
        partitions = {cp.partition for cp in results}
        assert partitions == {"0", "1"}

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            stream_id=checkpoint.stream_id,
            partition=checkpoint.partition,
            offset="99",
            metadata={},
        )
        store.save(updated)
        loaded = store.load(checkpoint.stream_id, checkpoint.partition)
        assert loaded.offset == "99"

    def test_metadata_is_preserved(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.stream_id, checkpoint.partition)
        assert loaded.metadata == checkpoint.metadata
