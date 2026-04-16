import pytest
from stream_checkpoint.backends import get_backend, list_backends


class TestRegistryUpstashTTL:
    def test_upstash_ttl_in_list(self):
        backends = list_backends()
        assert "upstash_ttl" in backends

    def test_get_upstash_ttl_backend_returns_class(self):
        from stream_checkpoint.backends.upstash_ttl_store import UpstashTTLCheckpointStore
        cls = get_backend("upstash_ttl")
        assert cls is UpstashTTLCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("nonexistent_backend_xyz")
