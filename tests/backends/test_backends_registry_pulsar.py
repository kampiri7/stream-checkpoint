"""Registry tests for the Pulsar backend."""

import pytest

from stream_checkpoint.backends import get_backend, list_backends
from stream_checkpoint.backends.pulsar_store import PulsarCheckpointStore


class TestRegistryPulsar:
    def test_pulsar_in_list(self):
        assert "pulsar" in list_backends()

    def test_get_pulsar_backend_returns_class(self):
        cls = get_backend("pulsar")
        assert cls is PulsarCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("nonexistent_backend_xyz")
