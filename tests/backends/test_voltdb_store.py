import json
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, call
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.voltdb_store import VoltDBCheckpointStore


class FakeCursor:
    """Minimal VoltDB client stub."""

    def __init__(self):
        self._store = {}
        self._last_query = None
        self._last_params = None

    def execute(self, query, params=None):
        self._last_query = query
        self._last_params = params

    def fetchone(self, query, params):
        key = (params[0], params[1])
        return self._store.get(key)

    def fetchall(self, query, params):
        pid = params[0]
        return [v for (p, _), v in self._store.items() if p == pid]

    def upsert(self, key, row):
        self._store[key] = row


@pytest.fixture()
def client():
    c = FakeCursor()
    # Pre-seed UPSERT so save() works through execute()
    original_execute = c.execute

    def smart_execute(query, params=None):
        original_execute(query, params)
        if params and "UPSERT" in query:
            key = (params[0], params[1])
            c._store[key] = tuple(params)

    c.execute = smart_execute
    return c


@pytest.fixture()
def store(client):
    return VoltDBCheckpointStore(client, table="checkpoints")


@pytest.fixture()
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        stream_id="stream1",
        offset={"partition": 0, "offset": 42},
        metadata={"source": "kafka"},
        updated_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    )


class TestVoltDBCheckpointStore:
    def test_save_and_load(self, store, client, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded is not None
        assert loaded.pipeline_id == "pipe1"
        assert loaded.stream_id == "stream1"
        assert loaded.offset == {"partition": 0, "offset": 42}

    def test_load_returns_none_for_missing(self, store):
        result = store.load("no_pipe", "no_stream")
        assert result is None

    def test_delete_removes_checkpoint(self, store, client, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        # Manually remove from fake store to simulate DELETE
        client._store.pop(("pipe1", "stream1"), None)
        assert store.load("pipe1", "stream1") is None

    def test_list_checkpoints(self, store, client, checkpoint):
        store.save(checkpoint)
        cp2 = Checkpoint(
            pipeline_id="pipe1",
            stream_id="stream2",
            offset={"partition": 1, "offset": 7},
            updated_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
        )
        store.save(cp2)
        results = store.list_checkpoints("pipe1")
        assert len(results) == 2

    def test_metadata_none_handled(self, store, client):
        cp = Checkpoint(
            pipeline_id="pipe2",
            stream_id="stream1",
            offset={"offset": 0},
            updated_at=datetime(2024, 3, 1, tzinfo=timezone.utc),
        )
        store.save(cp)
        row = client._store.get(("pipe2", "stream1"))
        assert row[3] is None

    def test_create_table_called_on_init(self):
        mock_client = MagicMock()
        VoltDBCheckpointStore(mock_client, table="test_tbl")
        mock_client.execute.assert_called_once()
        ddl_call = mock_client.execute.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS test_tbl" in ddl_call
