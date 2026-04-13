# Qdrant Backend

The `QdrantCheckpointStore` stores checkpoints as point payloads inside a
[Qdrant](https://qdrant.tech/) collection.  Each checkpoint is stored as a
single point whose payload contains all checkpoint fields.

## Installation

```bash
pip install qdrant-client
```

## Usage

```python
from qdrant_client import QdrantClient
from stream_checkpoint.backends.qdrant_store import QdrantCheckpointStore
from stream_checkpoint.base import Checkpoint
from datetime import datetime, timezone

client = QdrantClient(host="localhost", port=6333)

store = QdrantCheckpointStore(
    client=client,
    collection_name="stream_checkpoints",  # default
    prefix="ckpt",                          # default
)

checkpoint = Checkpoint(
    pipeline_id="my_pipeline",
    stream_id="topic_events",
    offset=1024,
    metadata={"partition": 0},
    updated_at=datetime.now(tz=timezone.utc),
)

# Save
store.save(checkpoint)

# Load
loaded = store.load("my_pipeline", "topic_events")
print(loaded.offset)  # 1024

# List all checkpoints for a pipeline
all_ckpts = store.list_checkpoints("my_pipeline")

# Delete
store.delete("my_pipeline", "topic_events")
```

## Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `client` | `QdrantClient` | — | Qdrant client instance |
| `collection_name` | `str` | `"stream_checkpoints"` | Qdrant collection to use |
| `prefix` | `str` | `"ckpt"` | Key prefix for point IDs |

## Notes

- The collection is created automatically on first use with a minimal
  1-dimensional dummy vector (checkpoints are stored purely as payload).
- Point IDs are deterministic hashes of `prefix:pipeline_id:stream_id`.
- `list_checkpoints` performs a payload text-filter scroll and may be slow
  on very large collections; consider adding a Qdrant payload index on
  `_ckpt_key` for production workloads.
