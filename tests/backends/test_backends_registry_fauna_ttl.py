import pytest
from stream_checkpoint.backends import list_backends, get_backend


class TestRegistryFaunaTTL:
    def test_fauna_ttl_in_list(self):
        assert "fauna_ttl" in list_backends()

    def test_get_fauna_ttl_backend_returns_class(self):
        from stream_checkpoint.backends.fauna_ttl_store import FaunaTTLCheckpointStore
        cls = get_backend("fauna_ttl")
        assert cls is FaunaTTLCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("not_a_real_backend_xyz")
