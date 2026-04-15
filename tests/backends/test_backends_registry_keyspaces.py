import pytest
from stream_checkpoint.backends import list_backends, get_backend


class TestRegistryKeyspaces:
    def test_keyspaces_in_list(self):
        backends = list_backends()
        assert "keyspaces" in backends

    def test_get_keyspaces_backend_returns_class(self):
        from stream_checkpoint.backends.keyspaces_store import KeyspacesCheckpointStore

        cls = get_backend("keyspaces")
        assert cls is KeyspacesCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("not_a_real_backend_xyz")
