"""Pluggable storage backends for stream-checkpoint."""

from stream_checkpoint.backends.memory_store import MemoryCheckpointStore
from stream_checkpoint.backends.file_store import FileCheckpointStore
from stream_checkpoint.backends.sqlite_store import SQLiteCheckpointStore

__all__ = [
    "MemoryCheckpointStore",
    "FileCheckpointStore",
    "SQLiteCheckpointStore",
    # Optional backends — imported lazily to avoid hard dependencies
    # "RedisCheckpointStore",
    # "PostgresCheckpointStore",
    # "DynamoDBCheckpointStore",
    # "MongoDBCheckpointStore",
    # "KafkaCheckpointStore",
    # "S3CheckpointStore",
    # "GCSCheckpointStore",
    # "AzureCheckpointStore",
    # "EtcdCheckpointStore",
    # "ZookeeperCheckpointStore",
]


def get_backend(name: str, **kwargs):
    """Factory function to retrieve a backend by name.

    Args:
        name: Backend identifier string (e.g. ``"redis"``, ``"zookeeper"``).
        **kwargs: Keyword arguments forwarded to the backend constructor.

    Returns:
        An instantiated :class:`~stream_checkpoint.base.BaseCheckpointStore`.

    Raises:
        ValueError: If *name* is not a recognised backend.
    """
    _registry = {
        "memory": "stream_checkpoint.backends.memory_store.MemoryCheckpointStore",
        "file": "stream_checkpoint.backends.file_store.FileCheckpointStore",
        "sqlite": "stream_checkpoint.backends.sqlite_store.SQLiteCheckpointStore",
        "redis": "stream_checkpoint.backends.redis_store.RedisCheckpointStore",
        "postgres": "stream_checkpoint.backends.postgres_store.PostgresCheckpointStore",
        "dynamodb": "stream_checkpoint.backends.dynamodb_store.DynamoDBCheckpointStore",
        "mongodb": "stream_checkpoint.backends.mongodb_store.MongoDBCheckpointStore",
        "kafka": "stream_checkpoint.backends.kafka_store.KafkaCheckpointStore",
        "s3": "stream_checkpoint.backends.s3_store.S3CheckpointStore",
        "gcs": "stream_checkpoint.backends.gcs_store.GCSCheckpointStore",
        "azure": "stream_checkpoint.backends.azure_store.AzureCheckpointStore",
        "etcd": "stream_checkpoint.backends.etcd_store.EtcdCheckpointStore",
        "zookeeper": "stream_checkpoint.backends.zookeeper_store.ZookeeperCheckpointStore",
    }

    if name not in _registry:
        raise ValueError(
            f"Unknown backend '{name}'. Available backends: {sorted(_registry)}"
        )

    module_path, class_name = _registry[name].rsplit(".", 1)
    import importlib
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    return cls(**kwargs)
