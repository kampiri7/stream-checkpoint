"""Tests for CockroachDBCheckpointStore."""

import json
import pytest

from stream_checkpoint.backends.cockroachdb_store import CockroachDBCheckpointStore
from stream_checkpoint.base import Checkpoint


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeCursor:
    def __init__(self, db):
        self._db = db
        self._last_result = None

    def execute(self, sql, params=None):
        sql_stripped = " ".join(sql.split()).upper()
        if sql_stripped.startswith("CREATE TABLE"):
            return
        if sql_stripped.startswith("INSERT INTO"):
            stream_id, key, data = params
            self._db[(stream_id, key)] = data
        elif sql_stripped.startswith("SELECT DATA"):
            stream_id, key = params
            row = self._db.get((stream_id, key))
            self._last_result = (row,) if row is not None else None
        elif sql_stripped.startswith("DELETE FROM"):
            stream_id, key = params
            self._db.pop((stream_id, key), None)
        elif sql_stripped.startswith("SELECT KEY"):
            stream_id = params[0]
            self._last_result = [
                (k,) for (sid, k) in sorted(self._db) if sid == stream_id
            ]

    def fetchone(self):
        return self._last_result

    def fetchall(self):
        return self._last_result or []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class FakeConnection:
    def __init__(self):
        self._db = {}
        self._committed = False

    def cursor(self):
        return FakeCursor(self._db)

    def commit(self):
        self._committed = True


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def conn():
    return FakeConnection()


@pytest.fixture
def store(conn):
    return CockroachDBCheckpointStore(conn)


@pytest.fixture
def checkpoint():
    return Checkpoint(stream_id="orders", key="offset", value="42", metadata={"src": "kafka"})


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestCockroachDBCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.stream_id, checkpoint.key)
        assert loaded is not None
        assert loaded.stream_id == checkpoint.stream_id
        assert loaded.key == checkpoint.key
        assert loaded.value == checkpoint.value

    def test_load_returns_none_for_missing(self, store):
        assert store.load("ghost", "nope") is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.stream_id, checkpoint.key)
        assert store.load(checkpoint.stream_id, checkpoint.key) is None

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("missing", "key")  # should not raise

    def test_overwrite_updates_value(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(stream_id=checkpoint.stream_id, key=checkpoint.key, value="99")
        store.save(updated)
        loaded = store.load(checkpoint.stream_id, checkpoint.key)
        assert loaded.value == "99"

    def test_list_keys(self, store):
        store.save(Checkpoint(stream_id="s1", key="a", value="1"))
        store.save(Checkpoint(stream_id="s1", key="b", value="2"))
        store.save(Checkpoint(stream_id="s2", key="c", value="3"))
        keys = store.list_keys("s1")
        assert keys == ["a", "b"]

    def test_list_keys_empty(self, store):
        assert store.list_keys("nonexistent") == []

    def test_commit_called_on_save(self, conn, store, checkpoint):
        conn._committed = False
        store.save(checkpoint)
        assert conn._committed is True
