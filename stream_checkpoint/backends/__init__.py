"""Pluggable storage backends for stream-checkpoint."""

from stream_checkpoint.backends.file_store import FileCheckpointStore
from stream_checkpoint.backends.memory_store import MemoryCheckpointStore
from stream_checkpoint.backends.sqlite_store import SQLiteCheckpointStore

try:
    from stream_checkpoint.backends.redis_store import RedisCheckpointStore
except ImportError:  # pragma: no cover
    pass

try:
    from stream_checkpoint.backends.postgres_store import PostgresCheckpointStore
except ImportError:  # pragma: no cover
    pass

try:
    from stream_checkpoint.backends.dynamodb_store import DynamoDBCheckpointStore
except ImportError:  # pragma: no cover
    pass

try:
    from stream_checkpoint.backends.mongodb_store import MongoDBCheckpointStore
except ImportError:  # pragma: no cover
    pass

try:
    from stream_checkpoint.backends.kafka_store import KafkaCheckpointStore
except ImportError:  # pragma: no cover
    pass

__all__ = [
    "FileCheckpointStore",
    "MemoryCheckpointStore",
    "SQLiteCheckpointStore",
    "RedisCheckpointStore",
    "PostgresCheckpointStore",
    "DynamoDBCheckpointStore",
    "MongoDBCheckpointStore",
    "KafkaCheckpointStore",
]
