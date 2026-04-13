import pytest
from stream_checkpoint.backends import get_backend, list_backends


class TestRegistrySurrealDB:
    def test_surrealdb_in_list(self):
        backends = list_backends()
        assert "surrealdb" in backends

    def test_get_surrealdb_backend_returns_class(self):
        cls = get_backend("surrealdb")
        from stream_checkpoint.backends.surrealdb_store import SurrealDBCheckpointStore
        assert cls is SurrealDBCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("not_a_real_backend_xyz")
