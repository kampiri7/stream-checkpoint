import json
import pytest
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.cloudwatch_store import CloudWatchCheckpointStore


class FakeCloudWatchClient:
    def __init__(self):
        self._groups = set()
        self._streams: dict = {}  # (group, stream) -> list[event]
        self._tokens: dict = {}

    def create_log_group(self, logGroupName):
        if logGroupName in self._groups:
            raise Exception("already exists")
        self._groups.add(logGroupName)

    def create_log_stream(self, logGroupName, logStreamName):
        key = (logGroupName, logStreamName)
        if key in self._streams:
            raise Exception("already exists")
        self._streams[key] = []

    def put_log_events(self, logGroupName, logStreamName, logEvents, sequenceToken=None):
        key = (logGroupName, logStreamName)
        self._streams.setdefault(key, []).extend(logEvents)
        token = f"token-{len(self._streams[key])}"
        return {"nextSequenceToken": token}

    def get_log_events(self, logGroupName, logStreamName, limit=1, startFromHead=False):
        key = (logGroupName, logStreamName)
        if key not in self._streams:
            raise Exception("ResourceNotFoundException")
        events = self._streams[key]
        return {"events": events[-limit:] if events else []}

    def delete_log_stream(self, logGroupName, logStreamName):
        key = (logGroupName, logStreamName)
        self._streams.pop(key, None)

    def describe_log_streams(self, logGroupName, logStreamNamePrefix):
        results = []
        for (g, s) in self._streams:
            if g == logGroupName and s.startswith(logStreamNamePrefix):
                results.append({"logStreamName": s})
        return {"logStreams": results}


@pytest.fixture
def client():
    return FakeCloudWatchClient()


@pytest.fixture
def store(client):
    return CloudWatchCheckpointStore(client, log_group="test-group")


@pytest.fixture
def checkpoint():
    return Checkpoint(stream_id="orders", partition_key="p1", offset="42", metadata={})


class TestCloudWatchCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.stream_id, checkpoint.partition_key)
        assert loaded is not None
        assert loaded.offset == checkpoint.offset

    def test_load_returns_none_for_missing(self, store):
        result = store.load("no-stream", "no-partition")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.stream_id, checkpoint.partition_key)
        assert store.load(checkpoint.stream_id, checkpoint.partition_key) is None

    def test_overwrite_keeps_latest(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            stream_id=checkpoint.stream_id,
            partition_key=checkpoint.partition_key,
            offset="99",
            metadata={},
        )
        store.save(updated)
        loaded = store.load(checkpoint.stream_id, checkpoint.partition_key)
        assert loaded.offset == "99"

    def test_list_checkpoints(self, store):
        for pk in ["p1", "p2", "p3"]:
            store.save(Checkpoint(stream_id="s", partition_key=pk, offset=pk, metadata={}))
        checkpoints = store.list_checkpoints("s")
        assert len(checkpoints) == 3
        offsets = {c.offset for c in checkpoints}
        assert offsets == {"p1", "p2", "p3"}

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("ghost", "ghost-pk")  # should not raise
