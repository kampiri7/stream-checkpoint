"""Registry tests for the Upstash backend."""

import pytest

from stream_checkpoint.backends import get_backend, list_backends
from stream_checkpoint.backends.upstash_store import UpstashCheckpointStore


class TestRegistryUpstash:
    def test_upstash_in_list(self):
        assert "upstash" in list_backends()

    def test_get_upstash_backend_returns_class(self):
        cls = get_backend("upstash")
        assert cls is UpstashCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("nonexistent_backend_xyz")
