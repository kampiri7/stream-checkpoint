import pytest

from stream_checkpoint.backends import get_backend, list_backends
from stream_checkpoint.backends.aerospike_store import AerospikeCheckpointStore


class TestRegistryAerospike:
    def test_aerospike_in_list(self):
        assert "aerospike" in list_backends()

    def test_get_aerospike_backend_returns_class(self):
        cls = get_backend("aerospike")
        assert cls is AerospikeCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("not_a_real_backend_xyz")
