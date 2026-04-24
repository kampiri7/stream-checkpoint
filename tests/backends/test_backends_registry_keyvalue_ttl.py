"""Registry tests for the keyvalue_ttl backend."""

import pytest
from stream_checkpoint.backends import get_backend, list_backends


class TestRegistryKeyValueTTL:
    def test_keyvalue_ttl_in_list(self):
        assert "keyvalue_ttl" in list_backends()

    def test_get_keyvalue_ttl_backend_returns_class(self):
        from stream_checkpoint.backends.keyvalue_ttl_store import KeyValueTTLCheckpointStore
        cls = get_backend("keyvalue_ttl")
        assert cls is KeyValueTTLCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("__nonexistent_backend__")
