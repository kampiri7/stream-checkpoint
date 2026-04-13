"""Registry tests for the RocksDB backend."""

import pytest
from stream_checkpoint.backends import get_backend, list_backends


class TestRegistryRocksDB:
    def test_rocksdb_in_list(self):
        assert "rocksdb" in list_backends()

    def test_get_rocksdb_backend_returns_class(self):
        from stream_checkpoint.backends.rocksdb_store import RocksDBCheckpointStore

        cls = get_backend("rocksdb")
        assert cls is RocksDBCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("not_a_real_backend")
