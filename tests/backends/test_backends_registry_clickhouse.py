import pytest

from stream_checkpoint.backends import get_backend, list_backends


class TestRegistryClickHouse:
    def test_clickhouse_in_list(self):
        assert "clickhouse" in list_backends()

    def test_get_clickhouse_backend_returns_class(self):
        from stream_checkpoint.backends.clickhouse_store import (
            ClickHouseCheckpointStore,
        )

        cls = get_backend("clickhouse")
        assert cls is ClickHouseCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("not_a_real_backend")
