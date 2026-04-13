"""Registry tests for the LMDB backend."""

import pytest
from stream_checkpoint.backends import get_backend, list_backends
from stream_checkpoint.backends.lmdb_store import LMDBCheckpointStore


class TestRegistryLMDB:
    def test_lmdb_in_list(self):
        assert "lmdb" in list_backends()

    def test_get_lmdb_backend_returns_class(self):
        cls = get_backend("lmdb")
        assert cls is LMDBCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("nonexistent_backend_xyz")
