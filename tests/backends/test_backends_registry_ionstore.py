"""Registry tests for the IonStore backend."""
import pytest
from stream_checkpoint.backends import list_backends, get_backend


class TestRegistryIonStore:
    def test_ionstore_in_list(self):
        backends = list_backends()
        assert "ionstore" in backends

    def test_get_ionstore_backend_returns_class(self):
        cls = get_backend("ionstore")
        from stream_checkpoint.backends.ionstore_store import IonStoreCheckpointStore
        assert cls is IonStoreCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("nonexistent_backend_xyz")
