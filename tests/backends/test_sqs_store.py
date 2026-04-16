import json
import pytest
from datetime import datetime, timezone
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.sqs_store import SQSCheckpointStore


class NoSuchKey(Exception):
    pass


class FakeS3Exceptions:
    NoSuchKey = NoSuchKey


class FakeS3Client:
    def __init__(self):
        self._store = {}
        self.exceptions = FakeS3Exceptions()

    def put Key, Body):
        self._store[(Bucket, Key)] = Body

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self._store:
            raise No)
        return {"Body": type("B", (), {"read": lambda self: self._store[(Bucket, Key)]})()}

    def, Key):
        self._store.pop((Bucket, Key), None)

    def list_objects_v2(self, Bucket, Prefix):
        contents = [
            {"Key": k} for (b, k) in self._store if b == Bucket and k.startswith(Prefix)
        ]
        return {"Contents": contents}


class FakeSQSClient:
    def __init__(self):
        self.messages = []

    def send_message(self, QueueUrl, MessageBody):
        self.messages.append({"QueueUrl": QueueUrl, "MessageBody": MessageBody})


@pytest.fixture
def clients():
    s3 = FakeS3Client()
    # patch get_object to close over store
    store_ref = s3._store
    def get_object(Bucket, Key):
        if (Bucket, Key) not in store_ref:
            raise NoSuchKey(Key)
        raw = store_ref[(Bucket, Key)]
        class Body:
            def read(self):
                return raw
        return {"Body": Body()}
    s3.get_object = get_object
    return s3, FakeSQSClient()


@pytest.fixture
def store(clients):
    s3, sqs = clients
    return SQSCheckpointStore(s3, sqs, bucket="test-bucket", queue_url="https://sqs.example.com/queue")


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        stream_id="stream1",
        offset=42,
        metadata={"key": "val"},
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )


class TestSQSCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded.offset == 42
        assert loaded.pipeline_id == "pipe1"

    def test_load_missing_returns_none(self, store):
        assert store.load("pipe1", "missing") is None

    def test_save_publishes_sqs_message(self, clients, store, checkpoint):
        _, sqs = clients
        store.save(checkpoint)
        assert len(sqs.messages) == 1
        body = json.loads(sqs.messages[0]["MessageBody"])
        assert body["event"] == "checkpoint_saved"
        assert body["pipeline_id"] == "pipe1"
        assert body["offset"] == 42

    def test_delete(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None

    def test_list_checkpoints(self, store, checkpoint):
        store.save(checkpoint)
        cp2 = Checkpoint(pipeline_id="pipe1", stream_id="stream2", offset=10,
                         metadata={}, timestamp=datetime(2024, 1, 2, tzinfo=timezone.utc))
        store.save(cp2)
        results = store.list_checkpoints("pipe1")
        assert len(results) == 2
        offsets = {r.offset for r in results}
        assert offsets == {42, 10}
