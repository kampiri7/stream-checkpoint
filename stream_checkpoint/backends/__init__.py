"""Pluggable storage backends for stream-checkpoint."""

from stream_checkpoint.backends.redis_store import RedisCheckpointStore

__all__ = ["RedisCheckpointStore"]
