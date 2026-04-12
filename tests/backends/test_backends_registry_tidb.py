import pytest
from stream_checkpoint.backends import list_backends, get_backend
from stream_checkpoint.backends.tidb_store import TiDBCheckpointStore


class TestRegistryTiDB:
    def test_tidb_in_list(self):
        assert "tidb" in list_backends()

    def test_get_tidb_backend_returns_class(self):
        cls = get_backend("tidb")
        assert cls is TiDBCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("nonexistent_backend_xyz")
