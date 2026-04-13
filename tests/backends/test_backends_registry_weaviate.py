import pytest
from stream_checkpoint.backends import list_backends, get_backend


class TestRegistryWeaviate:
    def test_weaviate_in_list(self):
        backends = list_backends()
        assert "weaviate" in backends

    def test_get_weaviate_backend_returns_class(self):
        from stream_checkpoint.backends.weaviate_store import WeaviateCheckpointStore
        cls = get_backend("weaviate")
        assert cls is WeaviateCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("nonexistent_backend_xyz")
