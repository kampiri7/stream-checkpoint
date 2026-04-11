import pytest

from stream_checkpoint.backends import get_backend, list_backends


class TestListBackends:
    def test_returns_list(self):
        backends = list_backends()
        assert isinstance(backends, list)

    def test_is_sorted(self):
        backends = list_backends()
        assert backends == sorted(backends)

    def test_contains_core_backends(self):
        backends = list_backends()
        for expected in ("memory", "file", "sqlite", "redis", "rabbitmq"):
            assert expected in backends

    def test_rabbitmq_included(self):
        assert "rabbitmq" in list_backends()


class TestGetBackend:
    def test_returns_memory_class(self):
        cls = get_backend("memory")
        from stream_checkpoint.backends.memory_store import MemoryCheckpointStore
        assert cls is MemoryCheckpointStore

    def test_returns_file_class(self):
        cls = get_backend("file")
        from stream_checkpoint.backends.file_store import FileCheckpointStore
        assert cls is FileCheckpointStore

    def test_returns_sqlite_class(self):
        cls = get_backend("sqlite")
        from stream_checkpoint.backends.sqlite_store import SQLiteCheckpointStore
        assert cls is SQLiteCheckpointStore

    def test_returns_rabbitmq_class(self):
        cls = get_backend("rabbitmq")
        from stream_checkpoint.backends.rabbitmq_store import RabbitMQCheckpointStore
        assert cls is RabbitMQCheckpointStore

    def test_unknown_backend_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown backend"):
            get_backend("nonexistent_backend")

    def test_error_message_lists_available_backends(self):
        with pytest.raises(ValueError) as exc_info:
            get_backend("bad")
        assert "memory" in str(exc_info.value)
        assert "rabbitmq" in str(exc_info.value)
