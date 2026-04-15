import json
from datetime import datetime
from unittest.mock import MagicMock, call
import pytest

from stream_checkpoint.backends.keyspaces_store import KeyspacesCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeRow:
    def __init__(self, stream_id, partition_id, offset, metadata, updated_at):
        self.stream_id = stream_id
        self.partition_id = partition_id
        self.offset = offset
        self.metadata = metadata
        self.updated_at = updated_at


class FakeResultSet:
    def __init__(self, rows):
        self._rows = rows

    def one(self):
        return self._rows[0] if self._rows else None

    def __iter__(self):
        return iter(self._rows)


@pytest.fixture
def session():
    s = MagicMock()
    s.execute.return_value = FakeResultSet([])
    return s


@pytest.fixture
def store(session):
    return KeyspacesCheckpointStore(session, keyspace="test_ks", table="checkpoints")


@pytest.fixture
def checkpoint():
    return Checkpoint(
        stream_id="s1",
        partition_id="p1",
        offset="100",
        metadata={"key": "val"},
        updated_at=datetime(2024, 1, 1, 12, 0, 0),
    )


class TestKeyspacesCheckpointStore:
    def test_create_table_called_on_init(self, session):
        KeyspacesCheckpointStore(session, keyspace="ks", table="cp")
        assert session.execute.called
        ddl = session.execute.call_args_list[0][0][0]
        assert "CREATE TABLE IF NOT EXISTS" in ddl

    def test_save_executes_insert(self, store, session, checkpoint):
        store.save(checkpoint)
        # last call is the INSERT
        args = session.execute.call_args_list[-1][0]
        assert "INSERT INTO" in args[0]
        params = args[1]
        assert params[0] == "s1"
        assert params[1] == "p1"
        assert params[2] == "100"
        assert json.loads(params[3]) == {"key": "val"}

    def test_load_returns_checkpoint(self, store, session, checkpoint):
        row = FakeRow(
            stream_id="s1",
            partition_id="p1",
            offset="100",
            metadata=json.dumps({"key": "val"}),
            updated_at=datetime(2024, 1, 1, 12, 0, 0),
        )
        session.execute.return_value = FakeResultSet([row])
        result = store.load("s1", "p1")
        assert result is not None
        assert result.stream_id == "s1"
        assert result.offset == "100"
        assert result.metadata == {"key": "val"}

    def test_load_returns_none_for_missing(self, store, session):
        session.execute.return_value = FakeResultSet([])
        result = store.load("no", "no")
        assert result is None

    def test_delete_executes_delete(self, store, session):
        store.delete("s1", "p1")
        args = session.execute.call_args_list[-1][0]
        assert "DELETE FROM" in args[0]
        assert args[1] == ("s1", "p1")

    def test_list_checkpoints_returns_all(self, store, session):
        rows = [
            FakeRow("s1", "p0", "10", json.dumps({}), datetime.utcnow()),
            FakeRow("s1", "p1", "20", json.dumps({"x": 1}), datetime.utcnow()),
        ]
        session.execute.return_value = FakeResultSet(rows)
        results = store.list_checkpoints("s1")
        assert len(results) == 2
        assert results[0].partition_id == "p0"
        assert results[1].metadata == {"x": 1}

    def test_list_checkpoints_empty(self, store, session):
        session.execute.return_value = FakeResultSet([])
        results = store.list_checkpoints("missing")
        assert results == []
