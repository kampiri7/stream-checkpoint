import json
import pytest
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.weaviate_store import WeaviateCheckpointStore


# ---------------------------------------------------------------------------
# Fake Weaviate client
# ---------------------------------------------------------------------------

class FakeQueryBuilder:
    def __init__(self, store_ref, class_name, props):
        self._store = store_ref
        self._class_name = class_name
        self._where = None
        self._limit = 1000

    def with_where(self, where):
        self._where = where
        return self

    def with_limit(self, limit):
        self._limit = limit
        return self

    def do(self):
        results = []
        if self._where:
            op = self._where["operator"]
            path_key = self._where["path"][0]
            value = self._where.get("valueText", "")
            for obj in self._store._objects.get(self._class_name, []):
                obj_val = obj.get("properties", {}).get(path_key, "")
                if op == "Equal" and obj_val == value:
                    results.append({**obj["properties"], "_additional": {"id": obj["id"]}})
                elif op == "Like":
                    prefix = value.rstrip("*")
                    if obj_val.startswith(prefix):
                        results.append({**obj["properties"], "_additional": {"id": obj["id"]}})
        return {"data": {"Get": {self._class_name: results[:self._limit]}}}


class FakeQueryClient:
    def __init__(self, store_ref):
        self._store = store_ref

    def get(self, class_name, props):
        return FakeQueryBuilder(self._store, class_name, props)


class FakeDataObjectClient:
    def __init__(self, store_ref):
        self._store = store_ref
        self._counter = 0

    def create(self, data_object, class_name):
        self._counter += 1
        obj_id = str(self._counter)
        self._store._objects.setdefault(class_name, []).append(
            {"id": obj_id, "properties": data_object}
        )
        return obj_id

    def delete(self, obj_id, class_name):
        self._store._objects[class_name] = [
            o for o in self._store._objects.get(class_name, []) if o["id"] != obj_id
        ]


class FakeSchemaClient:
    def __init__(self, store_ref):
        self._store = store_ref

    def get(self):
        return {"classes": [{"class": c} for c in self._store._schema_classes]}

    def create_class(self, schema):
        self._store._schema_classes.add(schema["class"])


class FakeWeaviateClient:
    def __init__(self):
        self._objects = {}
        self._schema_classes = set()
        self.schema = FakeSchemaClient(self)
        self.data_object = FakeDataObjectClient(self)
        self.query = FakeQueryClient(self)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    return FakeWeaviateClient()


@pytest.fixture
def store(client):
    return WeaviateCheckpointStore(client)


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=42, metadata={"k": "v"})


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestWeaviateCheckpointStore:
    def test_ensure_class_creates_schema(self, client):
        WeaviateCheckpointStore(client)
        assert "StreamCheckpoint" in client._schema_classes

    def test_ensure_class_skips_if_exists(self, client):
        client._schema_classes.add("StreamCheckpoint")
        WeaviateCheckpointStore(client)  # should not raise
        assert "StreamCheckpoint" in client._schema_classes

    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded is not None
        assert loaded.pipeline_id == checkpoint.pipeline_id
        assert loaded.stream_id == checkpoint.stream_id
        assert loaded.offset == checkpoint.offset

    def test_load_returns_none_for_missing(self, store):
        result = store.load("no_pipe", "no_stream")
        assert result is None

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=99)
        store.save(updated)
        loaded = store.load("pipe1", "stream1")
        assert loaded.offset == 99

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None

    def test_delete_nonexistent_is_safe(self, store):
        store.delete("ghost", "stream")  # should not raise

    def test_list_checkpoints(self, store):
        store.save(Checkpoint(pipeline_id="pipe2", stream_id="s1", offset=1))
        store.save(Checkpoint(pipeline_id="pipe2", stream_id="s2", offset=2))
        store.save(Checkpoint(pipeline_id="other", stream_id="s3", offset=3))
        results = store.list_checkpoints("pipe2")
        assert len(results) == 2
        offsets = {r.offset for r in results}
        assert offsets == {1, 2}

    def test_custom_class_name(self, client):
        s = WeaviateCheckpointStore(client, class_name="MyCheckpoints")
        assert "MyCheckpoints" in client._schema_classes
        cp = Checkpoint(pipeline_id="p", stream_id="s", offset=7)
        s.save(cp)
        assert s.load("p", "s").offset == 7
