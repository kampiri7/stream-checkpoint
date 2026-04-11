import json
import pytest
from datetime import datetime
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.hbase_store import HBaseCheckpointStore, _DATA_COLUMN


class FakeTable:
    def __init__(self, store):
        self._store = store

    def put(self, row_key, data):
        self._store[row_key] = data

    def row(self, row_key):
        return self._store.get(row_key, {})

    def delete(self, row_key):
        self._store.pop(row_key, None)

    def scan(self, row_prefix=None):
        for key, value in self._store.items():
            if row_prefix is None or key.startswith(row_prefix):
                yield key, value


class FakeConnection:
    def __init__(self):
        self._tables = {}

    def tables(self):
        return [name.encode() for name in self._tables]

    def create_table(self, name, families):
        self._tables[name] = {}

    def table(self, name):
        if name not in self._tables:
            self._tables[name] = {}
        return FakeTable(self._tables[name])


@pytest.fixture
def connection():
    return FakeConnection()


@pytest.fixture
def store(connection):
    return HBaseCheckpointStore(connection, table_name="checkpoints")


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        stream_id="stream1",
        offset=10,
        metadata={"source": "kafka"},
        timestamp=datetime(2024, 6, 1, 0, 0, 0),
    )


class TestHBaseCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        result = store.load("pipe1", "stream1")
        assert result is not None
        assert result.offset == 10
        assert result.metadata == {"source": "kafka"}

    def test_load_returns_none_for_missing(self, store):
        assert store.load("ghost", "stream") is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None

    def test_list_checkpoints_filters_by_pipeline(self, store, checkpoint):
        store.save(checkpoint)
        cp2 = Checkpoint(
            pipeline_id="pipe1",
            stream_id="stream2",
            offset=20,
            metadata={},
            timestamp=datetime(2024, 6, 2, 0, 0, 0),
        )
        cp_other = Checkpoint(
            pipeline_id="pipe2",
            stream_id="stream1",
            offset=99,
            metadata={},
            timestamp=datetime(2024, 6, 3, 0, 0, 0),
        )
        store.save(cp2)
        store.save(cp_other)
        results = store.list_checkpoints("pipe1")
        assert len(results) == 2
        assert all(r.pipeline_id == "pipe1" for r in results)

    def test_table_created_on_init(self, connection):
        HBaseCheckpointStore(connection, table_name="new_table")
        assert "new_table" in [t.decode() for t in connection.tables()]
