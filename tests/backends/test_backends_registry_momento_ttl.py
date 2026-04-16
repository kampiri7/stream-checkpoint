import pytest
from stream_checkpoint.backends import list_backends, get_backend


class TestRegistryMomentoTTL:
    def test_momento_ttl_in_list(self):
        backends = list_backends()
        assert "momento_ttl" in backends

    def test_get_momento_ttl_backend_returns_class(self):
        cls = get_backend("momento_ttl")
        from stream_checkpoint.backends.momento_ttl_store import MomentoTTLCheckpointStore
        assert cls is MomentoTTLCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("nonexistent_backend_xyz")
