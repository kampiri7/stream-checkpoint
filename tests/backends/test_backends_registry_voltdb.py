import pytest
from stream_checkpoint.backends import list_backends, get_backend


class TestRegistryVoltDB:
    def test_voltdb_in_list(self):
        backends = list_backends()
        assert "voltdb" in backends

    def test_get_voltdb_backend_returns_class(self):
        cls = get_backend("voltdb")
        from stream_checkpoint.backends.voltdb_store import VoltDBCheckpointStore
        assert cls is VoltDBCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("__nonexistent_backend__")
