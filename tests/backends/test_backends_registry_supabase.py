"""Registry tests for the Supabase checkpoint store backend."""

import pytest

from stream_checkpoint.backends import get_backend, list_backends


class TestRegistrySupabase:
    def test_supabase_in_list(self):
        assert "supabase" in list_backends()

    def test_get_supabase_backend_returns_class(self):
        from stream_checkpoint.backends.supabase_store import SupabaseCheckpointStore

        cls = get_backend("supabase")
        assert cls is SupabaseCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("nonexistent_backend_xyz")
