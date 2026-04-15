"""Registry tests for the Neo4j backend."""

import pytest

from stream_checkpoint.backends import get_backend, list_backends
from stream_checkpoint.backends.neo4j_store import Neo4jCheckpointStore


class TestRegistryNeo4j:
    def test_neo4j_in_list(self):
        assert "neo4j" in list_backends()

    def test_get_neo4j_backend_returns_class(self):
        cls = get_backend("neo4j")
        assert cls is Neo4jCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("does_not_exist_xyz")
