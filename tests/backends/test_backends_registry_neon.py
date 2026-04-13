"""Registry tests for the Neon backend."""

import pytest
from stream_checkpoint.backends import list_backends, get_backend


class TestRegistryNeon:
    def test_neon_in_list(self):
        assert "neon" in list_backends()

    def test_get_neon_backend_returns_class(self):
        from stream_checkpoint.backends.neon_store import NeonCheckpointStore

        cls = get_backend("neon")
        assert cls is NeonCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("unknown_backend_xyz")
