"""Tests for the Supabase checkpoint store backend."""

import json
import pytest

from stream_checkpoint.backends.supabase_store import SupabaseCheckpointStore
from stream_checkpoint.base import Checkpoint


# ---------------------------------------------------------------------------
# Fake Supabase client
# ---------------------------------------------------------------------------

class FakeQueryBuilder:
    """Minimal chainable query builder that operates on an in-memory list."""

    def __init__(self, rows: list):
        self._rows = rows  # shared mutable list
        self._filters: dict = {}
        self._limit_n: Optional[int] = None

    def select(self, *_args):
        return self

    def eq(self, field, value):
        self._filters[field] = value
        return self

    def limit(self, n):
        self._limit_n = n
        return self

    def upsert(self, data, on_conflict=None):
        key = on_conflict or "pipeline_id"
        for i, row in enumerate(self._rows):
            if row.get(key) == data.get(key):
                self._rows[i] = data
                return self
        self._rows.append(data)
        return self

    def delete(self):
        return self

    def execute(self):
        result = self._rows
        for field, value in self._filters.items():
            result = [r for r in result if r.get(field) == value]
        if self._limit_n is not None:
            result = result[: self._limit_n]
        # For delete, remove matching rows from the shared list
        if not self._filters:
            return _FakeResponse(result)
        # Support delete: mutate shared list when filters applied
        ids_to_remove = {r["pipeline_id"] for r in result}
        self._rows[:] = [r for r in self._rows if r["pipeline_id"] not in ids_to_remove]
        return _FakeResponse(result)


from typing import Optional


class _FakeResponse:
    def __init__(self, data):
        self.data = data


class FakeTable:
    def __init__(self, rows):
        self._rows = rows

    def select(self, *args):
        return FakeQueryBuilder(self._rows)

    def upsert(self, data, on_conflict=None):
        return FakeQueryBuilder(self._rows).upsert(data, on_conflict)

    def delete(self):
        return FakeQueryBuilder(self._rows).delete()


class FakeSupabaseClient:
    def __init__(self):
        self._rows: list = []
        self.postgrest = self

    def schema(self, _name):
        return self

    def table(self, _name):
        return FakeTable(self._rows)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def store():
    client = FakeSupabaseClient()
    return SupabaseCheckpointStore(client=client, table="checkpoints")


@pytest.fixture()
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe-sb-1",
        offset={"partition": 0, "offset": 42},
        metadata={"source": "supabase-test"},
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSupabaseCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.pipeline_id)
        assert loaded is not None
        assert loaded.pipeline_id == checkpoint.pipeline_id
        assert loaded.offset == checkpoint.offset

    def test_load_returns_none_for_missing(self, store):
        assert store.load("does-not-exist") is None

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id=checkpoint.pipeline_id,
            offset={"partition": 0, "offset": 99},
            metadata={},
        )
        store.save(updated)
        loaded = store.load(checkpoint.pipeline_id)
        assert loaded.offset == {"partition": 0, "offset": 99}

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.pipeline_id)
        assert store.load(checkpoint.pipeline_id) is None

    def test_list_checkpoints_empty(self, store):
        assert store.list_checkpoints() == []

    def test_list_checkpoints_returns_all(self, store):
        for i in range(3):
            store.save(
                Checkpoint(
                    pipeline_id=f"pipe-{i}",
                    offset={"offset": i},
                    metadata={},
                )
            )
        result = store.list_checkpoints()
        assert len(result) == 3

    def test_metadata_persisted(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.pipeline_id)
        assert loaded.metadata == checkpoint.metadata
