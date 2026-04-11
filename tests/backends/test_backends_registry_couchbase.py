import pytest
from stream_checkpoint.backends import list_backends, get_backend


class TestRegistryCouchbase:
    def test_couchbase_in_list(self):
        backends = list_backends()
        assert "couchbase" in backends

    def test_get_couchbase_backend_returns_class(self):
        from stream_checkpoint.backends.couchbase_store import CouchbaseCheckpointStore

        cls = get_backend("couchbase")
        assert cls is CouchbaseCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("nonexistent_backend_xyz")
