import pytest
from stream_checkpoint.backends import list_backends, get_backend


class TestRegistryCloudWatch:
    def test_cloudwatch_in_list(self):
        backends = list_backends()
        assert "cloudwatch" in backends

    def test_get_cloudwatch_backend_returns_class(self):
        from stream_checkpoint.backends.cloudwatch_store import CloudWatchCheckpointStore
        cls = get_backend("cloudwatch")
        assert cls is CloudWatchCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("not_a_real_backend_xyz")
