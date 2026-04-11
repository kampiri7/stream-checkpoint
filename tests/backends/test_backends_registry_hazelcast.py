"""Registry tests for the Hazelcast backend."""

import pytest

from stream_checkpoint.backends import get_backend, list_backends
from stream_checkpoint.backends.hazelcast_store import HazelcastCheckpointStore


class TestRegistryHazelcast:
    def test_hazelcast_in_list(self):
        backends = list_backends()
        assert "hazelcast" in backends

    def test_get_hazelcast_backend_returns_class(self):
        cls = get_backend("hazelcast")
        assert cls is HazelcastCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("nonexistent_backend_xyz")
