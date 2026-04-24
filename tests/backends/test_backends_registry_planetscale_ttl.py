import pytest
from stream_checkpoint.backends import list_backends, get_backend


class TestRegistryPlanetScaleTTL:
    def test_planetscale_ttl_in_list(self):
        backends = list_backends()
        assert "planetscale_ttl" in backends

    def test_get_planetscale_ttl_backend_returns_class(self):
        from stream_checkpoint.backends.planetscale_ttl_store import PlanetScaleTTLCheckpointStore
        cls = get_backend("planetscale_ttl")
        assert cls is PlanetScaleTTLCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("nonexistent_backend_xyz")
