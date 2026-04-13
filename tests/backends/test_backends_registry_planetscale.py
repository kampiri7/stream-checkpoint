import pytest
from stream_checkpoint.backends import list_backends, get_backend
from stream_checkpoint.backends.planetscale_store import PlanetScaleCheckpointStore


class TestRegistryPlanetScale:
    def test_planetscale_in_list(self):
        backends = list_backends()
        assert "planetscale" in backends

    def test_get_planetscale_backend_returns_class(self):
        cls = get_backend("planetscale")
        assert cls is PlanetScaleCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("not_a_real_backend_xyz")
