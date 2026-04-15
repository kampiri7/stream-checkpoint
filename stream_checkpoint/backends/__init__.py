"""Registry of all available checkpoint storage backends."""

from stream_checkpoint.backends.memory_store import MemoryCheckpointStore
from stream_checkpoint.backends.file_store import FileCheckpointStore
from stream_checkpoint.backends.redis_store import RedisCheckpointStore
from stream_checkpoint.backends.redis_ttl_store import RedisTTLCheckpointStore
from stream_checkpoint.backends.sqlite_store import SQLiteCheckpointStore
from stream_checkpoint.backends.postgres_store import PostgresCheckpointStore
from stream_checkpoint.backends.dynamodb_store import DynamoDBCheckpointStore
from stream_checkpoint.backends.dynamodb_ttl_store import DynamoDBTTLCheckpointStore
from stream_checkpoint.backends.mongodb_store import MongoDBCheckpointStore
from stream_checkpoint.backends.kafka_store import KafkaCheckpointStore
from stream_checkpoint.backends.s3_store import S3CheckpointStore
from stream_checkpoint.backends.gcs_store import GCSCheckpointStore
from stream_checkpoint.backends.azure_store import AzureCheckpointStore
from stream_checkpoint.backends.etcd_store import EtcdCheckpointStore
from stream_checkpoint.backends.zookeeper_store import ZookeeperCheckpointStore
from stream_checkpoint.backends.consul_store import ConsulCheckpointStore
from stream_checkpoint.backends.firestore_store import FirestoreCheckpointStore
from stream_checkpoint.backends.firestore_ttl_store import FirestoreTTLCheckpointStore
from stream_checkpoint.backends.cassandra_store import CassandraCheckpointStore
from stream_checkpoint.backends.hbase_store import HBaseCheckpointStore
from stream_checkpoint.backends.rabbitmq_store import RabbitMQCheckpointStore
from stream_checkpoint.backends.elasticsearch_store import ElasticsearchCheckpointStore
from stream_checkpoint.backends.influxdb_store import InfluxDBCheckpointStore
from stream_checkpoint.backends.scylladb_store import ScyllaDBCheckpointStore
from stream_checkpoint.backends.memcached_store import MemcachedCheckpointStore
from stream_checkpoint.backends.couchdb_store import CouchDBCheckpointStore
from stream_checkpoint.backends.couchbase_store import CouchbaseCheckpointStore
from stream_checkpoint.backends.nats_store import NATSCheckpointStore
from stream_checkpoint.backends.pulsar_store import PulsarCheckpointStore
from stream_checkpoint.backends.hazelcast_store import HazelcastCheckpointStore
from stream_checkpoint.backends.valkey_store import ValkeyCheckpointStore
from stream_checkpoint.backends.tigris_store import TigrisCheckpointStore
from stream_checkpoint.backends.tigris_store_ttl import TigrisTTLCheckpointStore
from stream_checkpoint.backends.aerospike_store import AerospikeCheckpointStore
from stream_checkpoint.backends.faunaCheckpointStore
from stream_checkpoint.backends.cockroachdb_store import CockroachDBCheckpointStore
from stream_checkpoint.backends.bigtable_store import BigtableCheckpointStore
from stream_checkpoint.backends.spanner_store import SpannerCheckpointStore
from stream_checkpoint.backends.tidb_store import TiDBCheckpointStore
from stream_checkpoint.backends.keydb_store import KeyDBCheckpointStore
from stream_checkpoint.backends.upstash_store import UpstashCheckpointStore
from stream_checkpoint.backends.momento_store import MomentoCheckpointStore
from stream_checkpoint.backends.turso_store import TursoCheckpointStore
from stream_checkpoint.backends.surrealdb_store import SurrealDBCheckpointStore
from stream_checkpoint.backends.planetscale_store import PlanetScaleCheckpointStore
from stream_checkpoint.backends.neon_store import NeonCheckpointStore
from stream_checkpoint.backends.supabase_store import SupabaseCheckpointStore
from stream_checkpoint.backends.lmdb_store import LMDBCheckpointStore
from stream_checkpoint.backends.rethinkdb_store import RethinkDBCheckpointStore
from stream_checkpoint.backends.rocksdb_store import RocksDBCheckpointStore
from stream_checkpoint.backends.leveldb_store import LevelDBCheckpointStore
from stream_checkpoint.backends.clickhouse_store import ClickHouseCheckpointStore
from stream_checkpoint.backends.qdrant_store import QdrantCheckpointStore
from stream_checkpoint.backends.weaviate_store import WeaviateCheckpointStore
from stream_checkpoint.backends.arangodb_store import ArangoDBCheckpointStore
from stream_checkpoint.backends.tarantool_store import TarantoolCheckpointStore
from stream_checkpoint.backends.neo4j_store import Neo4jCheckpointStore
from stream_checkpoint.backends.voltdb_store import VoltDBCheckpointStore

_REGISTRY = {
    "aerospike": AerospikeCheckpointStore,
    "arangodb": ArangoDBCheckpointStore,
    "azure": AzureCheckpointStore,
    "bigtable": BigtableCheckpointStore,
    "cassandra": CassandraCheckpointStore,
    "clickhouse": ClickHouseCheckpointStore,
    "cockroachdb": CockroachDBCheckpointStore,
    "consul": ConsulCheckpointStore,
    "couchbase": CouchbaseCheckpointStore,
    "couchdb": CouchDBCheckpointStore,
    "dynamodb": DynamoDBCheckpointStore,
    "dynamodb_ttl": DynamoDBTTLCheckpointStore,
    "elasticsearch": ElasticsearchCheckpointStore,
    "etcd": EtcdCheckpointStore,
    "faunadb": FaunaDBCheckpointStore,
    "file": FileCheckpointStore,
    "firestore": FirestoreCheckpointStore,
    "firestore_ttl": FirestoreTTLCheckpointStore,
    "gcs": GCSCheckpointStore,
    "hbase": HBaseCheckpointStore,
    "hazelcast": HazelcastCheckpointStore,
    "influxdb": InfluxDBCheckpointStore,
    "kafka": KafkaCheckpointStore,
    "keydb": KeyDBCheckpointStore,
    "leveldb": LevelDBCheckpointStore,
    "lmdb": LMDBCheckpointStore,
    "memcached": MemcachedCheckpointStore,
    "memory": MemoryCheckpointStore,
    "momento": MomentoCheckpointStore,
    "mongodb": MongoDBCheckpointStore,
    "nats": NATSCheckpointStore,
    "neo4j": Neo4jCheckpointStore,
    "neon": NeonCheckpointStore,
    "planetscale": PlanetScaleCheckpointStore,
    "postgres": PostgresCheckpointStore,
    "pulsar": PulsarCheckpointStore,
    "qdrant": QdrantCheckpointStore,
    "rabbitmq": RabbitMQCheckpointStore,
    "redis": RedisCheckpointStore,
    "redis_ttl": RedisTTLCheckpointStore,
    "rethinkdb": RethinkDBCheckpointStore,
    "rocksdb": RocksDBCheckpointStore,
    "s3": S3CheckpointStore,
    "scylladb": ScyllaDBCheckpointStore,
    "spanner": SpannerCheckpointStore,
    "sqlite": SQLiteCheckpointStore,
    "supabase": SupabaseCheckpointStore,
    "surrealdb": SurrealDBCheckpointStore,
    "tarantool": TarantoolCheckpointStore,
    "tidb": TiDBCheckpointStore,
    "tigris": TigrisCheckpointStore,
    "tigris_ttl": TigrisTTLCheckpointStore,
    "turso": TursoCheckpointStore,
    "upstash": UpstashCheckpointStore,
    "valkey": ValkeyCheckpointStore,
    "voltdb": VoltDBCheckpointStore,
    "weaviate": WeaviateCheckpointStore,
    "zookeeper": ZookeeperCheckpointStore,
}


def list_backends() -> list:
    """Return a sorted list of registered backend names."""
    return sorted(_REGISTRY.keys())


def get_backend(name: str):
    """Return the checkpoint store class for *name*.

    Raises KeyError if the backend is not registered.
    """
    if name not in _REGISTRY:
        raise KeyError(f"Unknown backend: {name!r}. Available: {list_backends()}")
    return _REGISTRY[name]
