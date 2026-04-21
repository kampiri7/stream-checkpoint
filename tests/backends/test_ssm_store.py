"""Tests for the SSM Parameter Store backend."""

import json
import pytest
from datetime import datetime, timezone

from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.ssm_store import SSMCheckpointStore


# ---------------------------------------------------------------------------
# Fake SSM client
# ---------------------------------------------------------------------------

class _ParameterNotFound(Exception):
    pass


class _Exceptions:
    ParameterNotFound = _ParameterNotFound


class FakeSSMClient:
    def __init__(self):
        self._store: dict = {}
        self.exceptions = _Exceptions()

    def put_parameter(self, Name, Value, Type, Overwrite, Tier="Standard"):
        self._store[Name] = Value

    def get_parameter(self, Name):
        if Name not in self._store:
            raise _ParameterNotFound(Name)
        return {"Parameter": {"Name": Name, "Value": self._store[Name]}}

    def delete_parameter(self, Name):
        if Name not in self._store:
            raise _ParameterNotFound(Name)
        del self._store[Name]

    def get_paginator(self, operation):
        return _FakePaginator(self._store)


class _FakePaginator:
    def __init__(self, store):
        self._store = store

    def paginate(self, Path):
        params = [
            {"Name": k, "Value": v}
            for k, v in self._store.items()
            if k.startswith(Path)
        ]
        yield {"Parameters": params}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    return FakeSSMClient()


@pytest.fixture
def store(client):
    return SSMCheckpointStore(client, prefix="/sc-test")


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe-1",
        checkpoint_id="ckpt-1",
        offset=42,
        metadata={"source": "ssm"},
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSSMCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe-1", "ckpt-1")
        assert loaded is not None
        assert loaded.pipeline_id == checkpoint.pipeline_id
        assert loaded.checkpoint_id == checkpoint.checkpoint_id
        assert loaded.offset == checkpoint.offset

    def test_load_returns_none_for_missing(self, store):
        result = store.load("no-pipe", "no-ckpt")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe-1", "ckpt-1")
        assert store.load("pipe-1", "ckpt-1") is None

    def test_delete_missing_does_not_raise(self, store):
        store.delete("ghost", "ghost-ckpt")  # should not raise

    def test_list_checkpoints(self, store, checkpoint):
        ckpt2 = Checkpoint(pipeline_id="pipe-1", checkpoint_id="ckpt-2", offset=99)
        store.save(checkpoint)
        store.save(ckpt2)
        results = store.list_checkpoints("pipe-1")
        ids = {c.checkpoint_id for c in results}
        assert "ckpt-1" in ids
        assert "ckpt-2" in ids

    def test_list_checkpoints_empty(self, store):
        assert store.list_checkpoints("empty-pipe") == []

    def test_key_format(self, store):
        key = store._key("my-pipe", "my-ckpt")
        assert key == "/sc-test/my-pipe/my-ckpt"

    def test_overwrite_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id="pipe-1",
            checkpoint_id="ckpt-1",
            offset=999,
        )
        store.save(updated)
        loaded = store.load("pipe-1", "ckpt-1")
        assert loaded.offset == 999
