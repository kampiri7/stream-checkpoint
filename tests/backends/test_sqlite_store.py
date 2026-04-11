import pytest

from stream_checkpoint.backends.sqlite_store import SQLiteCheckpointStore
from stream_checkpoint.base import Checkpoint


@pytest.fixture
def store():
    """Provide a fresh in-memory SQLite store for each test."""
    s = SQLiteCheckpointStore(db_path=":memory:")
    yield s
    s.close()


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe-1",
        stream_id="stream-A",
        offset=42,
        metadata={"source": "kafka"},
    )


class TestSQLiteCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.stream_id)
        assert loaded is not None
        assert loaded.pipeline_id == checkpoint.pipeline_id
        assert loaded.stream_id == checkpoint.stream_id
        assert loaded.offset == checkpoint.offset
        assert loaded.metadata == checkpoint.metadata

    def test_load_returns_none_for_missing(self, store):
        result = store.load("nonexistent", "stream-X")
        assert result is None

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id=checkpoint.pipeline_id,
            stream_id=checkpoint.stream_id,
            offset=99,
            metadata={"updated": True},
        )
        store.save(updated)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.stream_id)
        assert loaded.offset == 99
        assert loaded.metadata == {"updated": True}

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.pipeline_id, checkpoint.stream_id)
        result = store.load(checkpoint.pipeline_id, checkpoint.stream_id)
        assert result is None

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("ghost-pipe", "ghost-stream")

    def test_list_checkpoints_returns_all_for_pipeline(self, store):
        cp1 = Checkpoint(pipeline_id="pipe-2", stream_id="s1", offset=1)
        cp2 = Checkpoint(pipeline_id="pipe-2", stream_id="s2", offset=2)
        cp3 = Checkpoint(pipeline_id="pipe-3", stream_id="s1", offset=3)
        for cp in (cp1, cp2, cp3):
            store.save(cp)

        results = store.list_checkpoints("pipe-2")
        assert len(results) == 2
        stream_ids = {r.stream_id for r in results}
        assert stream_ids == {"s1", "s2"}

    def test_list_checkpoints_empty_pipeline(self, store):
        results = store.list_checkpoints("no-such-pipeline")
        assert results == []

    def test_list_checkpoints_does_not_return_other_pipeline(self, store):
        """Ensure list_checkpoints is scoped strictly to the requested pipeline."""
        cp_target = Checkpoint(pipeline_id="pipe-A", stream_id="s1", offset=10)
        cp_other = Checkpoint(pipeline_id="pipe-B", stream_id="s1", offset=20)
        store.save(cp_target)
        store.save(cp_other)

        results = store.list_checkpoints("pipe-A")
        assert len(results) == 1
        assert results[0].pipeline_id == "pipe-A"

    def test_custom_table_name(self):
        s = SQLiteCheckpointStore(db_path=":memory:", table="my_checkpoints")
        cp = Checkpoint(pipeline_id="p", stream_id="s", offset=7)
        s.save(cp)
        loaded = s.load("p", "s")
        assert loaded.offset == 7
        s.close()
