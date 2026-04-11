"""Registry tests for the Valkey backend."""

import pytest

from stream_checkpoint.backends import get_backend, list_backends
from stream_checkpoint.backends.valkey_store import ValkeyCheckpointStore


class TestRegistryValkey:
    def test_valkey_in_list(self):
        assert "valkey" in list_backends()

    def test_get_valkey_backend_returns_class(self):
        cls = get_backend("valkey")
        assert cls is ValkeyCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("nonexistent_backend_xyz")
