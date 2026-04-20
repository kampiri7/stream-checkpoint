import json
import pytest
from unittest.mock import MagicMock, patch
from stream_checkpoint.backends.cockroachdb_ttl_store import CockroachDBTTLCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeCursor:
    def __init__(self):
        self._rows = []
        self.executed = []

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class FakeConn:
    def __init__(self):
        self._cursor = FakeCursor()
        self.committed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True


@pytest.fixture
def conn():
    return FakeConn()


@pytest.fixture
def store(conn):
    return CockroachDBTTLCheckpointStore(conn, table="test_checkpoints", ttl_seconds=3600)


@pytest.fixture
def checkpoint():
    return Checkpoint(stream_id="orders", partition=0, offset=42, metadata={"source": "kafka"})


class TestCockroachDBTTLCheckpointStore:
    def test_create_table_called_on_init(self, conn):
        CockroachDBTTLCheckpointStore(conn, table="cp")
        assert any("CREATE TABLE" in str(q) for q, _ in conn._cursor.executed)

    def test_save_upserts_row(self, store, conn, checkpoint):
        store.save(checkpoint)
        queries = [q for q, _ in conn._cursor.executed]
        assert any("UPSERT" in q for q in queries)
        assert conn.committed

    def test_load_returns_checkpoint(self, store, conn, checkpoint):
        conn._cursor._rows = [(json.dumps(checkpoint.to_dict()),)]
        result = store.load("orders", 0)
        assert result is not None
        assert result.stream_id == "orders"
        assert result.offset == 42

    def test_load_returns_none_when_missing(self, store, conn):
        conn._cursor._rows = []
        result = store.load("orders", 99)
        assert result is None

    def test_delete_removes_row(self, store, conn):
        store.delete("orders", 0)
        queries = [q for q, _ in conn._cursor.executed]
        assert any("DELETE" in q for q in queries)
        assert conn.committed

    def test_list_checkpoints_returns_all(self, store, conn, checkpoint):
        conn._cursor._rows = [
            (json.dumps(checkpoint.to_dict()),),
            (json.dumps(Checkpoint(stream_id="orders", partition=1, offset=10).to_dict()),),
        ]
        results = store.list_checkpoints("orders")
        assert len(results) == 2

    def test_prefix_is_applied_to_key(self, conn, checkpoint):
        s = CockroachDBTTLCheckpointStore(conn, prefix="ns:")
        s.save(checkpoint)
        params = [p for _, p in conn._cursor.executed if p is not None]
        assert any("ns:orders:0" in str(p) for p in params)

    def test_ttl_seconds_in_create_table(self, conn):
        CockroachDBTTLCheckpointStore(conn, ttl_seconds=7200)
        queries = [q for q, _ in conn._cursor.executed]
        assert any("7200" in q for q in queries)
