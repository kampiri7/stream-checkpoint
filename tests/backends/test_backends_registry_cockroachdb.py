"""Registry tests for the CockroachDB backend."""

import pytest

from stream_checkpoint.backends import get_backend, list_backends
from stream_checkpoint.backends.cockroachdb_store import CockroachDBCheckpointStore


class TestRegistryCockroachDB:
    def test_cockroachdb_in_list(self):
        assert "cockroachdb" in list_backends()

    def test_get_cockroachdb_backend_returns_class(self):
        cls = get_backend("cockroachdb")
        assert cls is CockroachDBCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("not_a_real_backend_xyz")
