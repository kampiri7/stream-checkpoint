"""Registry tests for the Spanner backend."""

import pytest
from stream_checkpoint.backends import list_backends, get_backend


class TestRegistrySpanner:
    def test_spanner_in_list(self):
        backends = list_backends()
        assert "spanner" in backends

    def test_get_spanner_backend_returns_class(self):
        cls = get_backend("spanner")
        from stream_checkpoint.backends.spanner_store import SpannerCheckpointStore
        assert cls is SpannerCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("not_a_real_backend_xyz")
