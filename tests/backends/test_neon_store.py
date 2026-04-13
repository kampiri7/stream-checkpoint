"""Tests for NeonCheckpointStore using a fake psycopg2 connection."""

import json
import pytest
from unittest.mock import MagicMock, patch

from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.neon_store import NeonCheckpointStore


class FakeCursor:
    def __init__(self):
        self._rows = []
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class FakeConnection:
    def __init__(self):
        self.autocommit = False
        self._cursor = FakeCursor()

    def cursor(self):
        return self._cursor

    def close(self):
        pass


@pytest.fixture
def fake_conn():
    return FakeConnection()


@pytest.fixture
def store(fake_conn):
    with patch("psycopg2.connect", return_value=fake_conn):
        s = NeonCheckpointStore(connection_string="postgresql://fake", table="ckpts")
    return s, fake_conn


@pytest.fixture
def checkpoint():
    return Checkpoint(stream_id="s1", partition_id="p1", offset="42", metadata={"k": "v"})


class TestNeonCheckpointStore:
    def test_save_executes_upsert(self, store, checkpoint):
        s, conn = store
        s.save(checkpoint)
        last_sql, params = conn._cursor.executed[-1]
        assert "INSERT INTO" in last_sql
        assert params[0] == "s1"
        assert params[1] == "p1"
        assert params[2] == "42"

    def test_load_returns_none_for_missing(self, store):
        s, conn = store
        conn._cursor._rows = []
        result = s.load("no_stream", "no_partition")
        assert result is None

    def test_load_returns_checkpoint(self, store, checkpoint):
        s, conn = store
        conn._cursor._rows = [("s1", "p1", "42", {"k": "v"})]
        result = s.load("s1", "p1")
        assert result is not None
        assert result.stream_id == "s1"
        assert result.offset == "42"

    def test_delete_executes_delete(self, store):
        s, conn = store
        s.delete("s1", "p1")
        last_sql, params = conn._cursor.executed[-1]
        assert "DELETE FROM" in last_sql
        assert params == ("s1", "p1")

    def test_list_checkpoints_returns_all(self, store):
        s, conn = store
        conn._cursor._rows = [
            ("s1", "p0", "10", {}),
            ("s1", "p1", "20", {}),
        ]
        results = s.list_checkpoints("s1")
        assert len(results) == 2
        assert results[0].partition_id == "p0"

    def test_close_calls_conn_close(self, store):
        s, conn = store
        closed = []
        conn.close = lambda: closed.append(True)
        s.close()
        assert closed
