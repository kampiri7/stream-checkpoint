# KeyValueTTL Backend

The `KeyValueTTLCheckpointStore` is a generic checkpoint backend that wraps any
client exposing a simple key-value interface with optional TTL (time-to-live)
support.

## Interface Requirements

Your client must implement:

```python
client.set(key: str, value: str, ttl_seconds: Optional[int]) -> None
client.get(key: str) -> Optional[str]
client.delete(key: str) -> None
client.keys(prefix: str) -> Iterable[str]
```

## Usage

```python
from stream_checkpoint.backends.keyvalue_ttl_store import KeyValueTTLCheckpointStore
from stream_checkpoint.base import Checkpoint

# Provide any compatible client
store = KeyValueTTLCheckpointStore(
    client=my_kv_client,
    prefix="checkpoint:",
    ttl=3600,  # seconds; None disables TTL
)

cp = Checkpoint(pipeline_id="etl", stream_id="orders", offset=512)
store.save(cp)

loaded = store.load("etl", "orders")
print(loaded.offset)  # 512

store.delete("etl", "orders")
```

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `client` | object | required | KV client instance |
| `prefix` | str | `"checkpoint:"` | Key namespace prefix |
| `ttl` | int \| None | `None` | TTL in seconds; `None` means no expiry |

## Notes

- Keys are namespaced as `{prefix}{pipeline_id}:{stream_id}`.
- `list_checkpoints(pipeline_id)` scans all keys under the pipeline prefix.
- Serialization uses JSON via `Checkpoint.to_dict()` / `Checkpoint.from_dict()`.
