import pytest
from stream_checkpoint.backends import list_backends, get_backend


class TestRegistryDuckDB:
    def test_duckdb_in_list(self):
        backends = list_backends()
        assert "duckdb" in backends

    def test_get_duckdb_backend_returns_class(self):
        cls = get_backend("duckdb")
        from stream_checkpoint.backends.duckdb_store import DuckDBCheckpointStore
        assert cls is DuckDBCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("nonexistent_backend_xyz")
