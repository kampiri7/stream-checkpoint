"""Pluggable storage backends for stream-checkpoint."""

from stream_checkpoint.backends.memory_store import MemoryCheckpointStore
from stream_checkpoint.backends.file_store import FileCheckpointStore
from stream_checkpoint.backends.sqlite_store import SQLiteCheckpointStore

__all__ = [
    "MemoryCheckpointStore",
    "FileCheckpointStore",
    "SQLiteCheckpointStore",
]

try:
    from stream_checkpoint.backends.redis_store import RedisCheckpointStore
    __all__.append("RedisCheckpointStore")
except ImportError:
    pass

try:
    from stream_checkpoint.backends.postgres_store import PostgresCheckpointStore
    __all__.append("PostgresCheckpointStore")
except ImportError:
    pass

try:
    from stream_checkpoint.backends.mongodb_store import MongoDBCheckpointStore
    __all__.append("MongoDBCheckpointStore")
except ImportError:
    pass

try:
    from stream_checkpoint.backends.dynamodb_store import DynamoDBCheckpointStore
    __all__.append("DynamoDBCheckpointStore")
except ImportError:
    pass

try:
    from stream_checkpoint.backends.kafka_store import KafkaCheckpointStore
    __all__.append("KafkaCheckpointStore")
except ImportError:
    pass

try:
    from stream_checkpoint.backends.s3_store import S3CheckpointStore
    __all__.append("S3CheckpointStore")
except ImportError:
    pass

try:
    from stream_checkpoint.backends.gcs_store import GCSCheckpointStore
    __all__.append("GCSCheckpointStore")
except ImportError:
    pass
