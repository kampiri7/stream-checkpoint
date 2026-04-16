"""Registry tests for the KV store backend."""
from stream_checkpoint.backends import list_backends, get_backend
from stream_checkpoint.backends.kv_store import KVCheckpointStore


class TestRegistryKV:
    def test_kv_in_list(self):
        assert "kv" in list_backends()

    def test_get_kv_backend_returns_class(self):
        cls = get_backend("kv")
        assert cls is KVCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        import pytest
        with pytest.raises(KeyError):
            get_backend("nonexistent_backend_xyz")
