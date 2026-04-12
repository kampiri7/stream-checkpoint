import pytest
from stream_checkpoint.backends import list_backends, get_backend


class TestRegistryFaunaDB:
    def test_faunadb_in_list(self):
        assert "faunadb" in list_backends()

    def test_get_faunadb_backend_returns_class(self):
        from stream_checkpoint.backends.faunadb_store import FaunaDBCheckpointStore

        cls = get_backend("faunadb")
        assert cls is FaunaDBCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("nonexistent_backend_xyz")
