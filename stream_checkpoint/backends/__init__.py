"""
stream_checkpoint.backends
~~~~~~~~~~~~~~~~~~~~~~~~~~
Pluggable storage backends for :class:`~stream_checkpoint.base.BaseCheckpointStore`.

Available backends
------------------
- :class:`MemoryCheckpointStore`  – in-process dict (testing / prototyping)
- :class:`FileCheckpointStore`    – local filesystem (JSON files)
- :class:`SQLiteCheckpointStore`  – embedded SQLite database
- :class:`RedisCheckpointStore`   – Redis key/value store
- :class:`PostgresCheckpointStore`– PostgreSQL (psycopg2)
- :class:`DynamoDBCheckpointStore`– Amazon DynamoDB (boto3)
- :class:`MongoDBCheckpointStore` – MongoDB (pymongo)
- :class:`KafkaCheckpointStore`   – Apache Kafka (confluent-kafka)
- :class:`S3CheckpointStore`      – Amazon S3 (boto3)
"""

from stream_checkpoint.backends.memory_store import MemoryCheckpointStore
from stream_checkpoint.backends.file_store import FileCheckpointStore
from stream_checkpoint.backends.sqlite_store import SQLiteCheckpointStore
from stream_checkpoint.backends.redis_store import RedisCheckpointStore
from stream_checkpoint.backends.postgres_store import PostgresCheckpointStore
from stream_checkpoint.backends.dynamodb_store import DynamoDBCheckpointStore
from stream_checkpoint.backends.mongodb_store import MongoDBCheckpointStore
from stream_checkpoint.backends.kafka_store import KafkaCheckpointStore
from stream_checkpoint.backends.s3_store import S3CheckpointStore

__all__ = [
    "MemoryCheckpointStore",
    "FileCheckpointStore",
    "SQLiteCheckpointStore",
    "RedisCheckpointStore",
    "PostgresCheckpointStore",
    "DynamoDBCheckpointStore",
    "MongoDBCheckpointStore",
    "KafkaCheckpointStore",
    "S3CheckpointStore",
]
