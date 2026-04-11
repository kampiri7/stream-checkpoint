import pytest
from stream_checkpoint.backends import list_backends, get_backend
from stream_checkpoint.backends.nats_store import NATSCheckpointStore


class TestRegistryNATS:
    def test_nats_in_list(self):
        backends = list_backends()
        assert "nats" in backends

    def test_get_nats_backend_returns_class(self):
        cls = get_backend("nats")
        assert cls is NATSCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("nonexistent_backend_xyz")
