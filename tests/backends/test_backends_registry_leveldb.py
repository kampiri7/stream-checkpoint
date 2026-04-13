import pytest
from stream_checkpoint.backends import list_backends, get_backend
from stream_checkpoint.backends.leveldb_store import LevelDBCheckpointStore


class TestRegistryLevelDB:
    def test_leveldb_in_list(self):
        assert "leveldb" in list_backends()

    def test_get_leveldb_backend_returns_class(self):
        cls = get_backend("leveldb")
        assert cls is LevelDBCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("not_a_real_backend_xyz")
