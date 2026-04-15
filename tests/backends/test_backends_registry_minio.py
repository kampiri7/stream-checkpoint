import pytest

from stream_checkpoint.backends import get_backend, list_backends


class TestRegistryMinIO:
    def test_minio_in_list(self):
        backends = list_backends()
        assert "minio" in backends

    def test_get_minio_backend_returns_class(self):
        from stream_checkpoint.backends.minio_store import MinIOCheckpointStore

        cls = get_backend("minio")
        assert cls is MinIOCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("nonexistent_backend_xyz")
