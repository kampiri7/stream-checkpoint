# Weaviate Backend

The `WeaviateCheckpointStore` backend persists checkpoints in a
[Weaviate](https://weaviate.io/) vector database instance.

## Installation

```bash
pip install weaviate-client
```

## Usage

```python
import weaviate
from stream_checkpoint.backends.weaviate_store import WeaviateCheckpointStore
from stream_checkpoint.base import Checkpoint

client = weaviate.Client("http://localhost:8080")
store = WeaviateCheckpointStore(client)

# Save a checkpoint
cp = Checkpoint(pipeline_id="etl-pipeline", stream_id="orders", offset=1024)
store.save(cp)

# Load a checkpoint
loaded = store.load("etl-pipeline", "orders")
print(loaded.offset)  # 1024

# Delete a checkpoint
store.delete("etl-pipeline", "orders")

# List all checkpoints for a pipeline
all_cp = store.list_checkpoints("etl-pipeline")
```

## Constructor Parameters

| Parameter    | Type              | Default             | Description                              |
|--------------|-------------------|---------------------|------------------------------------------|
| `client`     | `weaviate.Client` | **required**        | Connected Weaviate client instance.      |
| `class_name` | `str`             | `StreamCheckpoint`  | Weaviate class used to store checkpoints.|

## Schema

The store automatically creates a Weaviate class with the following properties
if it does not already exist:

| Property         | Data Type | Description                          |
|------------------|-----------|--------------------------------------|
| `checkpoint_key` | `text`    | Composite key: `pipeline_id::stream_id` |
| `payload`        | `text`    | JSON-serialised `Checkpoint` object. |

## Notes

- Each `save()` call deletes any existing object with the same key before
  inserting the new one, ensuring idempotent upsert behaviour.
- `list_checkpoints(pipeline_id)` uses a `Like` filter and returns up to
  1 000 results per call.
- This backend is suitable for use cases that already have a Weaviate
  deployment and want to co-locate checkpoint metadata with vector data.
