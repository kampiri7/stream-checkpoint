import pytest
from stream_checkpoint.backends import list_backends, get_backend


class TestRegistryCouchDB:
    def test_couchdb_in_list(self):
        backends = list_backends()
        assert "couchdb" in backends

    def test_get_couchdb_backend_returns_class(self):
        cls = get_backend("couchdb")
        from stream_checkpoint.backends.couchdb_store import CouchDBCheckpointStore
        assert cls is CouchDBCheckpointStore

    def test_couchdb_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("not_a_real_backend_xyz")
