"""Backend registry for stream-checkpoint."""

from typing import List, Type

from stream_checkpoint.base import BaseCheckpointStore

_REGISTRY = {
    "aerospike": "stream_checkpoint.backends.aerospike_store.AerospikeCheckpointStore",
    "azure": "stream_checkpoint.backends.azure_store.AzureCheckpointStore",
    "bigtable": "stream_checkpoint.backends.bigtable_store.BigtableCheckpointStore",
    "cassandra": "stream_checkpoint.backends.cassandra_store.CassandraCheckpointStore",
    "cockroachdb": "stream_checkpoint.backends.cockroachdb_store.CockroachDBCheckpointStore",
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
    "keydb": "stream_checkpoint.backends.keydb_store.KeyDBCheckpointStore",
    "memcached": "stream_checkpoint.backends.memcached_store.MemcachedCheckpointStore",
    "memory": "stream_checkpoint.backends.memory_store.MemoryCheckpointStore",
    "momento": "stream_checkpoint.backends.momento_store.MomentoCheckpointStore",
    "mongodb": "stream_checkpoint.backends.mongodb_store.MongoDBCheckpointStore",
    "nats": "stream_checkpoint.backends.nats_store.NATSCheckpointStore",
    "neon": "stream_checkpoint.backends.neon_store.NeonCheckpointStore",
    "planetscale": "stream_checkpoint.backends.planetscale_store.PlanetScaleCheckpointStore",
    "postgres": "stream_checkpoint.backends.postgres_store.PostgresCheckpointStore",
    "pulsar": "stream_checkpoint.backends.pulsar_store.PulsarCheckpointStore",
    "rabbitmq": "stream_checkpoint.backends.rabbitmq_store.RabbitMQCheckpointStore",
    "redis": "stream_checkpoint.backends.redis_store.RedisCheckpointStore",
    "redis_ttl": "stream_checkpoint.backends.redis_ttl_store.RedisTTLCheckpointStore",
    "s3": "stream_checkpoint.backends.s3_store.S3CheckpointStore",
    "scylladb": "stream_checkpoint.backends.scylladb_store.ScyllaDBCheckpointStore",
    "spanner": "stream_checkpoint.backends.spanner_store.SpannerCheckpointStore",
    "sqlite": "stream_checkpoint.backends.sqlite_store.SQLiteCheckpointStore",
    "surrealdb": "stream_checkpoint.backends.surrealdb_store.SurrealDBCheckpointStore",
    "tidb": "stream_checkpoint.backends.tidb_store.TiDBCheckpointStore",
    "tigris": "stream_checkpoint.backends.tigris_store.TigrisCheckpointStore",
    "tigris_ttl": "stream_checkpoint.backends.tigris_store_ttl.TigrisTTLCheckpointStore",
    "turso": "stream_checkpoint.backends.turso_store.TursoCheckpointStore",
    "upstash": "stream_checkpoint.backends.upstash_store.UpstashCheckpointStore",
    "valkey": "stream_checkpoint.backends.valkey_store.ValkeyCheckpointStore",
    "zookeeper": "stream_checkpoint.backends.zookeeper_store.ZookeeperCheckpointStore",
}


def list_backends() -> List[str]:
    """Return a sorted list of registered backend names."""
    return sorted(_REGISTRY.keys())


def get_backend(name: str) -> Type[BaseCheckpointStore]:
    """Return the backend class for the given name.

    Raises:
        KeyError: If the backend name is not registered.
    """
    if name not in _REGISTRY:
        raise KeyError(f"Unknown backend: {name!r}. Available: {list_backends()}")
    module_path, class_name = _REGISTRY[name].rsplit(".", 1)
    import importlib

    module = importlib.import_module(module_path)
    return getattr(module, class_name)
