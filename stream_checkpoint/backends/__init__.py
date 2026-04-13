"""Backend registry for stream-checkpoint."""

from typing import List, Type

from stream_checkpoint.base import BaseCheckpointStore

_REGISTRY: dict = {
    "memory": "stream_checkpoint.backends.memory_store.MemoryCheckpointStore",
    "file": "stream_checkpoint.backends.file_store.FileCheckpointStore",
    "sqlite": "stream_checkpoint.backends.sqlite_store.SQLiteCheckpointStore",
    "redis": "stream_checkpoint.backends.redis_store.RedisCheckpointStore",
    "redis_ttl": "stream_checkpoint.backends.redis_ttl_store.RedisTTLCheckpointStore",
    "postgres": "stream_checkpoint.backends.postgres_store.PostgresCheckpointStore",
    "dynamodb": "stream_checkpoint.backends.dynamodb_store.DynamoDBCheckpointStore",
    "dynamodb_ttl": "stream_checkpoint.backends.dynamodb_ttl_store.DynamoDBTTLCheckpointStore",
    "mongodb": "stream_checkpoint.backends.mongodb_store.MongoDBCheckpointStore",
    "kafka": "stream_checkpoint.backends.kafka_store.KafkaCheckpointStore",
    "s3": "stream_checkpoint.backends.s3_store.S3CheckpointStore",
    "gcs": "stream_checkpoint.backends.gcs_store.GCSCheckpointStore",
    "azure": "stream_checkpoint.backends.azure_store.AzureCheckpointStore",
    "etcd": "stream_checkpoint.backends.etcd_store.EtcdCheckpointStore",
    "zookeeper": "stream_checkpoint.backends.zookeeper_store.ZookeeperCheckpointStore",
    "consul": "stream_checkpoint.backends.consul_store.ConsulCheckpointStore",
    "firestore": "stream_checkpoint.backends.firestore_store.FirestoreCheckpointStore",
    "firestore_ttl": "stream_checkpoint.backends.firestore_ttl_store.FirestoreTTLCheckpointStore",
    "cassandra": "stream_checkpoint.backends.cassandra_store.CassandraCheckpointStore",
    "hbase": "stream_checkpoint.backends.hbase_store.HBaseCheckpointStore",
    "rabbitmq": "stream_checkpoint.backends.rabbitmq_store.RabbitMQCheckpointStore",
    "elasticsearch": "stream_checkpoint.backends.elasticsearch_store.ElasticsearchCheckpointStore",
    "influxdb": "stream_checkpoint.backends.influxdb_store.InfluxDBCheckpointStore",
    "scylladb": "stream_checkpoint.backends.scylladb_store.ScyllaDBCheckpointStore",
    "memcached": "stream_checkpoint.backends.memcached_store.MemcachedCheckpointStore",
    "couchdb": "stream_checkpoint.backends.couchdb_store.CouchDBCheckpointStore",
    "couchbase": "stream_checkpoint.backends.couchbase_store.CouchbaseCheckpointStore",
    "nats": "stream_checkpoint.backends.nats_store.NATSCheckpointStore",
    "pulsar": "stream_checkpoint.backends.pulsar_store.PulsarCheckpointStore",
    "hazelcast": "stream_checkpoint.backends.hazelcast_store.HazelcastCheckpointStore",
    "valkey": "stream_checkpoint.backends.valkey_store.ValkeyCheckpointStore",
    "tigris": "stream_checkpoint.backends.tigris_store.TigrisCheckpointStore",
    "tigris_ttl": "stream_checkpoint.backends.tigris_store_ttl.TigrisTTLCheckpointStore",
    "aerospike": "stream_checkpoint.backends.aerospike_store.AerospikeCheckpointStore",
    "faunadb": "stream_checkpoint.backends.faunadb_store.FaunaDBCheckpointStore",
    "cockroachdb": "stream_checkpoint.backends.cockroachdb_store.CockroachDBCheckpointStore",
    "bigtable": "stream_checkpoint.backends.bigtable_store.BigtableCheckpointStore",
    "spanner": "stream_checkpoint.backends.spanner_store.SpannerCheckpointStore",
    "tidb": "stream_checkpoint.backends.tidb_store.TiDBCheckpointStore",
    "keydb": "stream_checkpoint.backends.keydb_store.KeyDBCheckpointStore",
    "upstash": "stream_checkpoint.backends.upstash_store.UpstashCheckpointStore",
    "momento": "stream_checkpoint.backends.momento_store.MomentoCheckpointStore",
    "turso": "stream_checkpoint.backends.turso_store.TursoCheckpointStore",
    "surrealdb": "stream_checkpoint.backends.surrealdb_store.SurrealDBCheckpointStore",
    "planetscale": "stream_checkpoint.backends.planetscale_store.PlanetScaleCheckpointStore",
    "neon": "stream_checkpoint.backends.neon_store.NeonCheckpointStore",
}


def list_backends() -> List[str]:
    """Return a sorted list of registered backend names."""
    return sorted(_REGISTRY.keys())


def get_backend(name: str) -> Type[BaseCheckpointStore]:
    """Return the backend class for *name*.

    Raises:
        KeyError: If *name* is not a registered backend.
    """
    if name not in _REGISTRY:
        raise KeyError(f"Unknown backend: {name!r}. Available: {list_backends()}")
    module_path, class_name = _REGISTRY[name].rsplit(".", 1)
    import importlib
    module = importlib.import_module(module_path)
    return getattr(module, class_name)
