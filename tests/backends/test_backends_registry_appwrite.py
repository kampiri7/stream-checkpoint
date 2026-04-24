import pytest
from stream_checkpoint.backends import list_backends, get_backend


class TestRegistryAppwrite:
    def test_appwrite_in_list(self):
        assert "appwrite" in list_backends()

    def test_get_appwrite_backend_returns_class(self):
        from stream_checkpoint.backends.appwrite_store import AppwriteCheckpointStore

        cls = get_backend("appwrite")
        assert cls is AppwriteCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("__nonexistent_backend__")
