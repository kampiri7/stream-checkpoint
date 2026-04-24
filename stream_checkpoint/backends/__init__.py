"""Registry of all available checkpoint store backends."""

BACKENDS = {
    "aerospike": "stream_checkpoint.backends.aerospike_store.AerospikeCheckpointStore",
    "appwrite": "stream_checkpoint.backends.appwrite_store.AppwriteCheckpointStore",
    "arangodb": "stream_checkpoint.backends.arangodb_store.ArangoDBCheckpointStore",
    "azure": "stream_checkpoint.backends.azure_store.AzureCheckpointStore",
    "bigtable": "stream_checkpoint.backends.bigtable_store.BigtableCheckpointStore",
    "cassandra": "stream_checkpoint.backends.cassandra_store.CassandraCheckpointStore",
    "clickhouse": "stream_checkpoint.backends.clickhouse_store.ClickHouseCheckpointStore",
    "cloudflare_kv": "stream_checkpoint.backends.cloudflare_kv_store.CloudflareKVCheckpointStore",
    "cockroachdb": "stream_checkpoint.backends.cockroachdb_store.CockroachDBCheckpointStore",
    "cockroachdb_ttl": "stream_checkpoint.backends.cockroachdb_ttl_store.CockroachDBTTLCheckpointStore",
    "consul": "stream_checkpoint.backends.consul_store.ConsulCheckpointStore",
    "couchbase": "stream_checkpoint.backends.couchbase_store.CouchbaseCheckpointStore",
    "couchdb": "stream_checkpoint.backends.couchdb_store.CouchDBCheckpointStore",
    "dax": "stream_checkpoint.backends.dax_store.DAXCheckpointStore",
    "dragonfly": "stream_checkpoint.backends.dragonfly_store.DragonflyCheckpointStore",
    "dynamodb": "stream_checkpoint.backends.dynamodb_store.DynamoDBCheckpointStore",
    "dynamodb_ttl": "stream_checkpoint.backends.dynamodb_ttl_store.DynamoDBTTLCheckpointStore",
    "elasticsearch": "stream_checkpoint.backends.elasticsearch_store.ElasticsearchCheckpointStore",
    "etcd": "stream_checkpoint.backends.etcd_store.EtcdCheckpointStore",
    "fauna_ttl": "stream_checkpoint.backends.fauna_ttl_store.FaunaTTLCheckpointStore",
    "faunadb": "stream_checkpoint.backends.faunadb_store.FaunaDBCheckpointStore",
    "file": "stream_checkpoint.backends.file_store.FileCheckpointStore",
    "firestore": "stream_checkpoint.backends.firestore_store.FirestoreCheckpointStore",
    "firestore_ttl": "stream_checkpoint.backends.firestore_ttl_store.FirestoreTTLCheckpointStore",
    "garage": "stream_checkpoint.backends.garage_store.GarageCheckpointStore",
    "garnet": "stream_checkpoint.backends.garnet_store.GarnetCheckpointStore",
    "gcs": "stream_checkpoint.backends.gcs_store.GCSCheckpointStore",
    "hazelcast": "stream_checkpoint.backends.hazelcast_store.HazelcastCheckpointStore",
    "hbase": "stream_checkpoint.backends.hbase_store.HBaseCheckpointStore",
    "influxdb": "stream_checkpoint.backends.influxdb_store.InfluxDBCheckpointStore",
    "ionstore": "stream_checkpoint.backends.ionstore_store.IonStoreCheckpointStore",
    "kafka": "stream_checkpoint.backends.kafka_store.KafkaCheckpointStore",
    "keydb": "stream_checkpoint.backends.keydb_store.KeyDBCheckpointStore",
    "keyspaces": "stream_checkpoint.backends.keyspaces_store.KeyspacesCheckpointStore",
    "kv": "stream_checkpoint.backends.kv_store.KVCheckpointStore",
    "leveldb": "stream_checkpoint.backends.leveldb_store.LevelDBCheckpointStore",
    "lmdb": "stream_checkpoint.backends.lmdb_store.LMDBCheckpointStore",
    "memory": "stream_checkpoint.backends.memory_store.MemoryCheckpointStore",
    "memcached": "stream_checkpoint.backends.memcached_store.MemcachedCheckpointStore",
    "minio": "stream_checkpoint.backends.minio_store.MinIOCheckpointStore",
    "momento": "stream_checkpoint.backends.momento_store.MomentoCheckpointStore",
    "momento_ttl": "stream_checkpoint.backends.momento_ttl_store.MomentoTTLCheckpointStore",
    "mongodb": "stream_checkpoint.backends.mongodb_store.MongoDBCheckpointStore",
    "nats": "stream_checkpoint.backends.nats_store.NATSCheckpointStore",
    "neo4j": "stream_checkpoint.backends.neo4j_store.Neo4jCheckpointStore",
    "neon": "stream_checkpoint.backends.neon_store.NeonCheckpointStore",
    "opensearch": "stream_checkpoint.backends.opensearch_store.OpenSearchCheckpointStore",
    "planetscale": "stream_checkpoint.backends.planetscale_store.PlanetScaleCheckpointStore",
    "planetscale_ttl": "stream_checkpoint.backends.planetscale_ttl_store.PlanetScaleTTLCheckpointStore",
    "pocketbase": "stream_checkpoint.backends.pocketbase_store.PocketBaseCheckpointStore",
    "postgres": "stream_checkpoint.backends.postgres_store.PostgresCheckpointStore",
    "pulsar": "stream_checkpoint.backends.pulsar_store.PulsarCheckpointStore",
    "qdrant": "stream_checkpoint.backends.qdrant_store.QdrantCheckpointStore",
    "rabbitmq": "stream_checkpoint.backends.rabbitmq_store.RabbitMQCheckpointStore",
    "redict": "stream_checkpoint.backends.redict_store.RedictCheckpointStore",
    "redis": "stream_checkpoint.backends.redis_store.RedisCheckpointStore",
    "redis_ttl": "stream_checkpoint.backends.redis_ttl_store.RedisTTLCheckpointStore",
    "replicaset": "stream_checkpoint.backends.replicaset_store.MongoDBReplicaSetCheckpointStore",
    "rethinkdb": "stream_checkpoint.backends.rethinkdb_store.RethinkDBCheckpointStore",
    "rocksdb": "stream_checkpoint.backends.rocksdb_store.RocksDBCheckpointStore",
    "s3": "stream_checkpoint.backends.s3_store.S3CheckpointStore",
    "scylladb": "stream_checkpoint.backends.scylladb_store.ScyllaDBCheckpointStore",
    "spanner": "stream_checkpoint.backends.spanner_store.SpannerCheckpointStore",
    "sqlite": "stream_checkpoint.backends.sqlite_store.SQLiteCheckpointStore",
    "sqs": "stream_checkpoint.backends.sqs_store.SQSCheckpointStore",
    "ssm": "stream_checkpoint.backends.ssm_store.SSMCheckpointStore",
    "supabase": "stream_checkpoint.backends.supabase_store.SupabaseCheckpointStore",
    "surrealdb": "stream_checkpoint.backends.surrealdb_store.SurrealDBCheckpointStore",
    "tarantool": "stream_checkpoint.backends.tarantool_store.TarantoolCheckpointStore",
    "tidb": "stream_checkpoint.backends.tidb_store.TiDBCheckpointStore",
    "tigris": "stream_checkpoint.backends.tigris_store.TigrisCheckpointStore",
    "tigris_ttl": "stream_checkpoint.backends.tigris_store_ttl.TigrisTTLCheckpointStore",
    "turso": "stream_checkpoint.backends.turso_store.TursoCheckpointStore",
    "upstash": "stream_checkpoint.backends.upstash_store.UpstashCheckpointStore",
    "upstash_ttl": "stream_checkpoint.backends.upstash_ttl_store.UpstashTTLCheckpointStore",
    "valkey": "stream_checkpoint.backends.valkey_store.ValkeyCheckpointStore",
    "vercel_kv": "stream_checkpoint.backends.vercel_kv_store.VercelKVCheckpointStore",
    "voltdb": "stream_checkpoint.backends.voltdb_store.VoltDBCheckpointStore",
    "weaviate": "stream_checkpoint.backends.weaviate_store.WeaviateCheckpointStore",
    "zookeeper": "stream_checkpoint.backends.zookeeper_store.ZookeeperCheckpointStore",
}


def list_backends():
    """Return a sorted list of all registered backend names."""
    return sorted(BACKENDS.keys())


def get_backend(name: str):
    """Return the checkpoint store class for the given backend name."""
    if name not in BACKENDS:
        raise KeyError(f"Unknown backend: {name!r}. Available: {list_backends()}")
    module_path, class_name = BACKENDS[name].rsplit(".", 1)
    import importlib
    module = importlib.import_module(module_path)
    return getattr(module, class_name)
