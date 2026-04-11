from typing import Dict, Type
from stream_checkpoint.base import BaseCheckpointStore

_BACKENDS: Dict[str, str] = {
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
    "cassandra": "stream_checkpoint.backends.cassandra_store.CassandraCheckpointStore",
    "hbase": "stream_checkpoint.backends.hbase_store.HBaseCheckpointStore",
}


def list_backends():
    """Return a list of all registered backend names."""
    return list(_BACKENDS.keys())


def get_backend(name: str) -> Type[BaseCheckpointStore]:
    """Retrieve a backend class by name.

    Args:
        name: The registered backend identifier (e.g. ``"redis"``).

    Returns:
        The corresponding :class:`BaseCheckpointStore` subclass.

    Raises:
        KeyError: If *name* is not a registered backend.
        ImportError: If the backend's dependencies are not installed.
    """
    if name not in _BACKENDS:
        raise KeyError(
            f"Unknown backend '{name}'. Available backends: {list_backends()}"
        )
    module_path, class_name = _BACKENDS[name].rsplit(".", 1)
    import importlib
    module = importlib.import_module(module_path)
    return getattr(module, class_name)
