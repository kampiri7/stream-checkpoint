import pytest
from stream_checkpoint.backends import list_backends, get_backend


class TestRegistrySQS:
    def test_sqs_in_list(self):
        assert "sqs" in list_backends()

    def test_get_sqs_backend_returns_class(self):
        from stream_checkpoint.backends.sqs_store import SQSCheckpointStore
        cls = get_backend("sqs")
        assert cls is SQSCheckpointStore

    def test_list_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_get_unknown_backend_raises(self):
        with pytest.raises(KeyError):
            get_backend("nonexistent_backend_xyz")
