# Upstash Backend

The `UpstashCheckpointStore` stores checkpoints in [Upstash Redis](https://upstash.com/),
a serverless Redis service accessible via a REST API client.

## Installation

```bash
pip install upstash-redis
```

## Usage

```python
from upstash_redis import Redis
from stream_checkpoint.backends.upstash_store import UpstashCheckpointStore
from stream_checkpoint.base import Checkpoint

client = Redis(url="https://<your-endpoint>.upstash.io", token="<your-token>")

store = UpstashCheckpointStore(client, prefix="myapp", ttl=86400)

cp = Checkpoint(pipeline_id="etl-pipeline", stream_id="topic-orders", offset=1024)
store.save(cp)

loaded = store.load("etl-pipeline", "topic-orders")
print(loaded.offset)  # 1024
```

## Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `client` | `upstash_redis.Redis` | — | Upstash Redis client instance |
| `prefix` | `str` | `"checkpoint"` | Key prefix for all stored checkpoints |
| `ttl` | `int \| None` | `None` | Optional TTL in seconds; `None` disables expiration |

## Key Format

Keys are stored as:

```
<prefix>:<pipeline_id>:<stream_id>
```

## Methods

### `save(checkpoint)`
Persists a `Checkpoint` object.  Uses `SETEX` when `ttl` is configured,
otherwise `SET`.

### `load(pipeline_id, stream_id) -> Checkpoint | None`
Retrieves a checkpoint by pipeline and stream identifiers.  Returns
`None` if no checkpoint exists.

### `delete(pipeline_id, stream_id)`
Removes the checkpoint entry.  Safe to call when the key does not exist.

### `list_checkpoints(pipeline_id) -> list[Checkpoint]`
Returns all checkpoints for a given `pipeline_id` using a key-pattern
scan (`KEYS <prefix>:<pipeline_id>:*`).

> **Note:** `KEYS` scans the entire keyspace and may be slow on large
> databases.  Consider using a dedicated Upstash database per pipeline
> for production workloads.
