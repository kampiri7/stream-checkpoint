"""Tests for the Neo4j checkpoint store backend."""

import json
import pytest

from stream_checkpoint.backends.neo4j_store import Neo4jCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeRecord:
    def __init__(self, data: str):
        self._data = data

    def __getitem__(self, key):
        if key == "data":
            return self._data
        raise KeyError(key)


class FakeResult:
    def __init__(self, records):
        self._records = records
        self._iter = iter(records)

    def single(self):
        return self._records[0] if self._records else None

    def __iter__(self):
        return iter(self._records)


class FakeSession:
    def __init__(self, store: dict):
        self._store = store
        self._queries = []

    def run(self, query, **params):
        self._queries.append((query, params))
        if "RETURN c.data" in query and "MATCH" in query:
            cid = params.get("cid")
            if cid in self._store:
                return FakeResult([FakeRecord(self._store[cid])])
            return FakeResult([])
        if "MERGE" in query or "SET" in query:
            cid = params.get("cid")
            self._store[cid] = params.get("data")
        if "DETACH DELETE" in query and "cid" in params:
            self._store.pop(params["cid"], None)
        if "MATCH (c:Checkpoint) RETURN" in query:
            records = [FakeRecord(v) for v in sorted(self._store.values())]
            return FakeResult(records)
        if "MATCH (c:Checkpoint) DETACH" in query:
            self._store.clear()
        return FakeResult([])

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class FakeDriver:
    def __init__(self):
        self._store = {}

    def session(self, database=None):
        return FakeSession(self._store)


@pytest.fixture
def driver():
    return FakeDriver()


@pytest.fixture
def store(driver):
    return Neo4jCheckpointStore(driver, database="neo4j")


@pytest.fixture
def checkpoint():
    return Checkpoint(checkpoint_id="cp-1", stream_id="stream-A", offset=42)


class TestNeo4jCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("cp-1")
        assert loaded is not None
        assert loaded.checkpoint_id == "cp-1"
        assert loaded.stream_id == "stream-A"
        assert loaded.offset == 42

    def test_load_returns_none_for_missing(self, store):
        assert store.load("nonexistent") is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("cp-1")
        assert store.load("cp-1") is None

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("ghost")

    def test_list_checkpoints(self, store):
        cp1 = Checkpoint(checkpoint_id="cp-1", stream_id="s1", offset=1)
        cp2 = Checkpoint(checkpoint_id="cp-2", stream_id="s2", offset=2)
        store.save(cp1)
        store.save(cp2)
        results = store.list_checkpoints()
        assert len(results) == 2

    def test_clear_removes_all(self, store):
        store.save(Checkpoint(checkpoint_id="cp-1", stream_id="s", offset=0))
        store.save(Checkpoint(checkpoint_id="cp-2", stream_id="s", offset=1))
        store.clear()
        assert store.load("cp-1") is None
        assert store.load("cp-2") is None

    def test_overwrite_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(checkpoint_id="cp-1", stream_id="stream-A", offset=99)
        store.save(updated)
        loaded = store.load("cp-1")
        assert loaded.offset == 99
