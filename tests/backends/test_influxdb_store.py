import json
import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone

from stream_checkpoint.backends.influxdb_store import InfluxDBCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeRecord:
    def __init__(self, value):
        self._value = value

    def get_value(self):
        return self._value


class FakeTable:
    def __init__(self, records):
        self.records = records


class FakeQueryAPI:
    def __init__(self, tables):
        self._tables = tables

    def query(self, flux, org=None):
        return self._tables


class FakeWriteAPI:
    def __init__(self):
        self.written = []

    def write(self, bucket, org, record):
        self.written.append(record)


class FakeDeleteAPI:
    def __init__(self):
        self.calls = []

    def delete(self, start, stop, predicate, bucket=None, org=None):
        self.calls.append((start, stop, predicate, bucket, org))


class FakeInfluxClient:
    def __init__(self, tables=None):
        self._write_api = FakeWriteAPI()
        self._query_api = FakeQueryAPI(tables or [])
        self._delete_api = FakeDeleteAPI()

    def write_api(self):
        return self._write_api

    def query_api(self):
        return self._query_api

    def delete_api(self):
        return self._delete_api


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        stream_id="stream1",
        offset=42,
        metadata={"source": "kafka"},
    )


@pytest.fixture
def empty_client():
    return FakeInfluxClient(tables=[])


@pytest.fixture
def store(empty_client):
    return InfluxDBCheckpointStore(
        client=empty_client, bucket="checkpoints", org="myorg"
    )


class TestInfluxDBCheckpointStore:
    def test_save_writes_record(self, store, checkpoint, empty_client):
        store.save(checkpoint)
        assert len(empty_client._write_api.written) == 1
        record = empty_client._write_api.written[0]
        assert "pipe1" in record
        assert "stream1" in record

    def test_load_returns_none_when_no_data(self, store):
        result = store.load("pipe1", "stream1")
        assert result is None

    def test_load_returns_checkpoint(self, checkpoint):
        data = json.dumps(checkpoint.to_dict())
        tables = [FakeTable([FakeRecord(data)])]
        client = FakeInfluxClient(tables=tables)
        s = InfluxDBCheckpointStore(client=client, bucket="b", org="o")
        result = s.load("pipe1", "stream1")
        assert result is not None
        assert result.pipeline_id == "pipe1"
        assert result.offset == 42

    def test_delete_calls_delete_api(self, store, empty_client):
        store.delete("pipe1", "stream1")
        assert len(empty_client._delete_api.calls) == 1
        _, _, predicate, bucket, org = empty_client._delete_api.calls[0]
        assert "pipe1" in predicate
        assert "stream1" in predicate
        assert bucket == "checkpoints"
        assert org == "myorg"

    def test_list_checkpoints_returns_empty(self, store):
        result = store.list_checkpoints("pipe1")
        assert result == []

    def test_list_checkpoints_returns_all(self, checkpoint):
        data = json.dumps(checkpoint.to_dict())
        tables = [FakeTable([FakeRecord(data)])]
        client = FakeInfluxClient(tables=tables)
        s = InfluxDBCheckpointStore(client=client, bucket="b", org="o")
        result = s.list_checkpoints("pipe1")
        assert len(result) == 1
        assert result[0].stream_id == "stream1"

    def test_custom_measurement(self, checkpoint):
        client = FakeInfluxClient()
        s = InfluxDBCheckpointStore(
            client=client, bucket="b", org="o", measurement="custom_cp"
        )
        s.save(checkpoint)
        record = client._write_api.written[0]
        assert record.startswith("custom_cp,")
