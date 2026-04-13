"""Registry tests for FirestoreTTLCheckpointStore."""

import pytest

from stream_checkpoint.backends import get_backend, list_backends
from stream_checkpoint.backends.firestore_ttl_store import FirestoreTTLCheckpointStore


class TestRegistryFirestoreTTL:
    def test_firestore_ttl_in_list(self):
        assert "firestore_ttl" in list_backends()

    def test_get_firestore_ttl_backend_returns_class(self):
        cls = get_backend("firestore_ttl")
        assert cls is FirestoreTTLCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("not_a_real_backend_xyz")
