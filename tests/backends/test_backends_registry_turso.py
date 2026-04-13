"""Registry tests for the Turso backend."""

import pytest
from stream_checkpoint.backends import list_backends, get_backend
from stream_checkpoint.backends.turso_store import TursoCheckpointStore


class TestRegistryTurso:
    def test_turso_in_list(self):
        assert "turso" in list_backends()

    def test_get_turso_backend_returns_class(self):
        cls = get_backend("turso")
        assert cls is TursoCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("not_a_real_backend_xyz")
