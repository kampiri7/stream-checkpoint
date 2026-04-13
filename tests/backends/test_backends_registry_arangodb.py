import pytest
from stream_checkpoint.backends import list_backends, get_backend


class TestRegistryArangoDB:
    def test_arangodb_in_list(self):
        backends = list_backends()
        assert "arangodb" in backends

    def test_get_arangodb_backend_returns_class(self):
        from stream_checkpoint.backends.arangodb_store import ArangoDBCheckpointStore
        cls = get_backend("arangodb")
        assert cls is ArangoDBCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("not_a_real_backend_xyz")
