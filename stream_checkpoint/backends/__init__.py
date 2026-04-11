"""Registry of available checkpoint store backends."""

from typing import Any, Dict


_BACKENDS = {
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
    "consul": "stream_checkpoint.backends.consul_store.ConsulCheckpointStore",
    "firestore": "stream_checkpoint.backends.firestore_store.FirestoreCheckpointStore",
}


def get_backend(name: str, **kwargs: Any):
    """
    Instantiate a checkpoint store backend by name.

    :param name: Backend identifier (e.g. ``"redis"``, ``"firestore"``).
    :param kwargs: Constructor arguments forwarded to the backend class.
    :returns: An instance of the requested :class:`BaseCheckpointStore`.
    :raises ValueError: If *name* is not a registered backend.
    """
    if name not in _BACKENDS:
        available = ", ".join(sorted(_BACKENDS))
        raise ValueError(
            f"Unknown backend '{name}'. Available backends: {available}"
        )

    module_path, class_name = _BACKENDS[name].rsplit(".", 1)
    import importlib
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    return cls(**kwargs)


def list_backends() -> list:
    """Return a sorted list of registered backend names."""
    return sorted(_BACKENDS.keys())
