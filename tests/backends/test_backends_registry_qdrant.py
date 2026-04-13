"""Registry tests for the Qdrant backend."""

import pytest
from stream_checkpoint.backends import list_backends, get_backend


class TestRegistryQdrant:
    def test_qdrant_in_list(self):
        assert "qdrant" in list_backends()

    def test_get_qdrant_backend_returns_class(self):
        from stream_checkpoint.backends.qdrant_store import QdrantCheckpointStore

        cls = get_backend("qdrant")
        assert cls is QdrantCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("does_not_exist_xyz")
