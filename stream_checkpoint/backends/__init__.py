"""Backend registry for stream-checkpoint."""

from typing import List, Type

from stream_checkpoint.base import BaseCheckpointStore

_REGISTRY = {
    "aerospike": "stream_checkpoint.backends.aerospike_store.AerospikeCheckpointStore",
    "azure": "stream_checkpoint.backends.azure_store.AzureCheckpointStore",
    "cassandra": "stream_checkpoint.backends.cassandra_store.CassandraCheckpointStore",
    "consul": "stream_checkpoint.backends.consul_store.ConsulCheckpointStore",
    "couchbase": "stream_checkpoint.backends.couchbase_store.CouchbaseCheckpointStore",
    "couchdb": "stream_checkpoint.backends.couchdb_store.CouchDBCheckpointStore",
    "dynamodb": "stream_checkpoint.backends.dynamodb_store.DynamoDBCheckpointStore",
    "dynamodb_ttl": "stream_checkpoint.backends.dynamodb_ttl_store.DynamoDBTTLCheckpointStore",
    "elasticsearch": "stream_checkpoint.backends.elasticsearch_store.ElasticsearchCheckpointStore",
    "etcd": "stream_checkpoint.backends.etcd_store.EtcdCheckpointStore",
    "faunadb": "stream_checkpoint.backends.faunadb_store.FaunaDBCheckpointStore",
    "file": "stream_checkpoint.backends.file_store.FileCheckpointStore",
    "firestore": "stream_checkpoint.backends.firestore_store.FirestoreCheckpointStore",
    "gcs": "stream_checkpoint.backends.gcs_store.GCSCheckpointStore",
    "hazelcast": "stream_checkpoint.backends.hazelcast_store.HazelcastCheckpointStore",
    "hbase": "stream_checkpoint.backends.hbase_store.HBaseCheckpointStore",
    "influxdb": "stream_checkpoint.backends.influxdb_store.InfluxDBCheckpointStore",
    "kafka": "stream_checkpoint.backends.kafka_store.KafkaCheckpointStore",
    "memcached": "stream_checkpoint.backends.memcached_store.MemcachedCheckpointStore",
    "memory": "stream_checkpoint.backends.memory_store.MemoryCheckpointStore",
    "mongodb": "stream_checkpoint.backends.mongodb_store.MongoDBCheckpointStore",
    "nats": "stream_checkpoint.backends.nats_store.NATSCheckpointStore",
    "postgres": "stream_checkpoint.backends.postgres_store.PostgresCheckpointStore",
    "pulsar": "stream_checkpoint.backends.pulsar_store.PulsarCheckpointStore",
    "rabbitmq": "stream_checkpoint.backends.rabbitmq_store.RabbitMQCheckpointStore",
    "redis": "stream_checkpoint.backends.redis_store.RedisCheckpointStore",
    "redis_ttl": "stream_checkpoint.backends.redis_ttl_store.RedisTTLCheckpointStore",
    "s3": "stream_checkpoint.backends.s3_store.S3CheckpointStore",
    "scylladb": "stream_checkpoint.backends.scylladb_store.ScyllaDBCheckpointStore",
    "sqlite": "stream_checkpoint.backends.sqlite_store.SQLiteCheckpointStore",
    "tigris": "stream_checkpoint.backends.tigris_store.TigrisCheckpointStore",
    "tigris_ttl": "stream_checkpoint.backends.tigris_store_ttl.TigrisTTLCheckpointStore",
    "valkey": "stream_checkpoint.backends.valkey_store.ValkeyCheckpointStore",
    "zookeeper": "stream_checkpoint.backends.zookeeper_store.ZookeeperCheckpointStore",
}


def list_backends() -> List[str]:
    """Return a sorted list of registered backend names."""
    return sorted(_REGISTRY.keys())


def get_backend(name: str) -> Type[BaseCheckpointStore]:
    """Return the checkpoint store class for *name*.

    Raises:
        KeyError: If *name* is not a registered backend.
    """
    if name not in _REGISTRY:
        raise KeyError(f"Unknown backend: {name!r}. Available: {list_backends()}")
    module_path, class_name = _REGISTRY[name].rsplit(".", 1)
    import importlib

    module = importlib.import_module(module_path)
    return getattr(module, class_name)
