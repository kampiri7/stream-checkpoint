"""Registry tests for TTL-enabled backend stores."""

import pytest

from stream_checkpoint.backends import list_backends, get_backend


class TestRegistryDynamoDBTTL:
    def test_dynamodb_ttl_in_list(self):
        backends = list_backends()
        assert "dynamodb_ttl" in backends

    def test_get_dynamodb_ttl_backend_returns_class(self):
        cls = get_backend("dynamodb_ttl")
        from stream_checkpoint.backends.dynamodb_ttl_store import DynamoDBTTLCheckpointStore
        assert cls is DynamoDBTTLCheckpointStore


class TestRegistryRedisTTL:
    def test_redis_ttl_in_list(self):
        backends = list_backends()
        assert "redis_ttl" in backends

    def test_get_redis_ttl_backend_returns_class(self):
        cls = get_backend("redis_ttl")
        from stream_checkpoint.backends.redis_ttl_store import RedisTTLCheckpointStore
        assert cls is RedisTTLCheckpointStore

    def test_redis_ttl_is_subclass_of_base(self):
        from stream_checkpoint.base import BaseCheckpointStore
        from stream_checkpoint.backends.redis_ttl_store import RedisTTLCheckpointStore
        assert issubclass(RedisTTLCheckpointStore, BaseCheckpointStore)

    def test_dynamodb_ttl_is_subclass_of_base(self):
        from stream_checkpoint.base import BaseCheckpointStore
        from stream_checkpoint.backends.dynamodb_ttl_store import DynamoDBTTLCheckpointStore
        assert issubclass(DynamoDBTTLCheckpointStore, BaseCheckpointStore)
