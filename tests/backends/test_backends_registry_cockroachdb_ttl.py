import pytest
from stream_checkpoint.backends import list_backends, get_backend


class TestRegistryCockroachDBTTL:
    def test_cockroachdb_ttl_in_list(self):
        backends = list_backends()
        assert "cockroachdb_ttl" in backends

    def test_get_cockroachdb_ttl_backend_returns_class(self):
        cls = get_backend("cockroachdb_ttl")
        from stream_checkpoint.backends.cockroachdb_ttl_store import (
            CockroachDBTTLCheckpointStore,
        )
        assert cls is CockroachDBTTLCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("not_a_real_backend")
