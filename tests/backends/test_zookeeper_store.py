import json
import pytest
from datetime import datetime, timezone

from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.zookeeper_store import ZookeeperCheckpointStore


# ---------------------------------------------------------------------------
# Fake Kazoo helpers
# ---------------------------------------------------------------------------

class FakeKazooClient:
    """Minimal in-memory stand-in for kazoo.client.KazooClient."""

    def __init__(self, hosts="", timeout=10.0):
        self._store: dict[str, bytes] = {}

    def start(self):
        pass

    def stop(self):
        pass

    def close(self):
        pass

    def ensure_path(self, path: str):
        pass

    def exists(self, path: str) -> bool:
        return path in self._store

    def create(self, path: str, data: bytes):
        self._store[path] = data

    def set(self, path: str, data: bytes):
        self._store[path] = data

    def get(self, path: str):
        return self._store[path], None

    def delete(self, path: str):
        self._store.pop(path, None)

    def get_children(self, path: str):
        prefix = path.rstrip("/") + "/"
        return [
            k[len(prefix):].split("/")[0]
            for k in self._store
            if k.startswith(prefix)
        ]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def store(monkeypatch):
    fake_client = FakeKazooClient()

    class FakeKazooModule:
        class client:
            KazooClient = lambda *a, **kw: fake_client  # noqa: E731

    monkeypatch.setitem(__import__("sys").modules, "kazoo", FakeKazooModule())
    monkeypatch.setitem(__import__("sys").modules, "kazoo.client", FakeKazooModule.client())

    s = ZookeeperCheckpointStore.__new__(ZookeeperCheckpointStore)
    s._root = "/stream_checkpoint"
    s._client = fake_client
    return s


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        stream_id="stream1",
        offset=42,
        metadata={"topic": "events"},
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestZookeeperCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.stream_id)
        assert loaded is not None
        assert loaded.offset == checkpoint.offset
        assert loaded.pipeline_id == checkpoint.pipeline_id

    def test_load_returns_none_for_missing(self, store):
        result = store.load("no_pipe", "no_stream")
        assert result is None

    def test_overwrite_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id=checkpoint.pipeline_id,
            stream_id=checkpoint.stream_id,
            offset=99,
            metadata={},
            timestamp=checkpoint.timestamp,
        )
        store.save(updated)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.stream_id)
        assert loaded.offset == 99

    def test_delete(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.pipeline_id, checkpoint.stream_id)
        assert store.load(checkpoint.pipeline_id, checkpoint.stream_id) is None

    def test_delete_nonexistent_is_safe(self, store):
        store.delete("ghost_pipe", "ghost_stream")

    def test_list_checkpoints(self, store):
        cp1 = Checkpoint(pipeline_id="pipe2", stream_id="s1", offset=1, metadata={}, timestamp=datetime.now(timezone.utc))
        cp2 = Checkpoint(pipeline_id="pipe2", stream_id="s2", offset=2, metadata={}, timestamp=datetime.now(timezone.utc))
        store.save(cp1)
        store.save(cp2)
        results = store.list_checkpoints("pipe2")
        offsets = {cp.offset for cp in results}
        assert offsets == {1, 2}

    def test_list_checkpoints_empty_pipeline(self, store):
        assert store.list_checkpoints("nonexistent_pipe") == []
