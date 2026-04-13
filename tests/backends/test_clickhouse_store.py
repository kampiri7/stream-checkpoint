import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, call

import pytest

from stream_checkpoint.backends.clickhouse_store import ClickHouseCheckpointStore
from stream_checkpoint.base import Checkpoint


# ---------------------------------------------------------------------------
# Fake ClickHouse client
# ---------------------------------------------------------------------------

class FakeClickHouseClient:
    """Minimal stand-in for clickhouse_driver.Client."""

    def __init__(self):
        self._store: dict[str, dict] = {}
        self._deleted: set = set()

    def execute(self, query: str, params=None):
        q = query.strip()

        if q.startswith("CREATE TABLE"):
            return []

        if q.startswith("INSERT INTO"):
            row = params[0]
            self._store[row["stream_id"]] = row
            return []

        if q.startswith("SELECT stream_id, offset"):
            sid = params["stream_id"]
            if sid not in self._store or sid in self._deleted:
                return []
            r = self._store[sid]
            return [(r["stream_id"], r["offset"], r["metadata"], r["updated_at"])]

        if q.startswith("ALTER TABLE") and "DELETE" in q:
            sid = params["stream_id"]
            self._deleted.add(sid)
            return []

        if q.startswith("SELECT DISTINCT"):
            return [(k,) for k in self._store if k not in self._deleted]

        return []


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def client():
    return FakeClickHouseClient()


@pytest.fixture()
def store(client):
    return ClickHouseCheckpointStore(client, database="testdb", table="cp")


@pytest.fixture()
def checkpoint():
    return Checkpoint(
        stream_id="topic-1",
        offset="42",
        metadata={"partition": 0},
        updated_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestClickHouseCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("topic-1")
        assert loaded is not None
        assert loaded.stream_id == "topic-1"
        assert loaded.offset == "42"
        assert loaded.metadata == {"partition": 0}

    def test_load_returns_none_for_missing(self, store):
        assert store.load("nonexistent") is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("topic-1")
        assert store.load("topic-1") is None

    def test_list_streams(self, store, checkpoint):
        store.save(checkpoint)
        cp2 = Checkpoint(stream_id="topic-2", offset="7")
        store.save(cp2)
        streams = store.list_streams()
        assert set(streams) == {"topic-1", "topic-2"}

    def test_list_streams_excludes_deleted(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("topic-1")
        assert "topic-1" not in store.list_streams()

    def test_create_table_called_on_init(self, client):
        mock_client = MagicMock()
        ClickHouseCheckpointStore(mock_client)
        mock_client.execute.assert_called_once()
        ddl = mock_client.execute.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in ddl
