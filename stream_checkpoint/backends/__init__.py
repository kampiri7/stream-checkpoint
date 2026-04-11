"""Registry of available checkpoint store backends."""
from __future__ import annotations

BACKEND_REGISTRY: dict[str, str] = {
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
    "rabbitmq": "stream_checkpoint.backends.rabbitmq_store.RabbitMQCheckpointStore",
}


def list_backends() -> list[str]:
    """Return the names of all registered backends."""
    return sorted(BACKEND_REGISTRY.keys())


def get_backend(name: str):
    """Import and return the class for the given backend name.

    :param name: One of the keys in BACKEND_REGISTRY.
    :raises ValueError: If the backend name is not recognised.
    :raises ImportError: If the backend's optional dependency is not installed.
    """
    if name not in BACKEND_REGISTRY:
        available = ", ".join(sorted(BACKEND_REGISTRY))
        raise ValueError(
            f"Unknown backend '{name}'. Available backends: {available}"
        )

    module_path, class_name = BACKEND_REGISTRY[name].rsplit(".", 1)
    import importlib

    module = importlib.import_module(module_path)
    return getattr(module, class_name)
