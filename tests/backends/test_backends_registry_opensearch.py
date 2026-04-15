import pytest
from stream_checkpoint.backends import list_backends, get_backend


class TestRegistryOpenSearch:
    def test_opensearch_in_list(self):
        backends = list_backends()
        assert "opensearch" in backends

    def test_get_opensearch_backend_returns_class(self):
        from stream_checkpoint.backends.opensearch_store import OpenSearchCheckpointStore
        cls = get_backend("opensearch")
        assert cls is OpenSearchCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("not_a_real_backend_xyz")
