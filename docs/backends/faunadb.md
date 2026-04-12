# FaunaDB Backend

The `FaunaDBCheckpointStore` persists checkpoints in a [Fauna](https://fauna.com/) (formerly FaunaDB) collection.

## Requirements

Install the official Fauna Python driver:

```bash
pip install fauna
```

## Usage

```python
from fauna import Client
from stream_checkpoint.backends.faunadb_store import FaunaDBCheckpointStore
from stream_checkpoint.base import Checkpoint

client = Client(secret="fn_your_secret_here")

store = FaunaDBCheckpointStore(
    client=client,
    collection="checkpoints",          # Fauna collection name
    index="checkpoints_by_pipeline_id", # Index for pipeline_id lookups
)

# Save a checkpoint
cp = Checkpoint(pipeline_id="my-pipeline", offset=1024, metadata={"shard": 3})
store.save(cp)

# Load it back
loaded = store.load("my-pipeline")
print(loaded.offset)  # 1024

# List all checkpoints
for c in store.list_checkpoints():
    print(c.pipeline_id, c.offset)

# Delete
store.delete("my-pipeline")
```

## Configuration

| Parameter    | Type   | Default                          | Description                              |
|--------------|--------|----------------------------------|------------------------------------------|
| `client`     | object | **required**                     | Authenticated `fauna.Client` instance.   |
| `collection` | `str`  | `"checkpoints"`                  | Fauna collection to store documents in.  |
| `index`      | `str`  | `"checkpoints_by_pipeline_id"`   | Fauna index used to look up by pipeline. |

## Notes

- The collection and index must be created in Fauna before first use.  
  You can create them via the Fauna Dashboard or the FQL shell:

  ```fql
  Collection.create({ name: "checkpoints" })
  Index.create({
    name: "checkpoints_by_pipeline_id",
    source: Collection("checkpoints"),
    terms: [{ field: ["data", "pipeline_id"] }],
    unique: true
  })
  ```

- Checkpoint documents are stored with the full serialised dict under the `data` field.
- `save()` performs an upsert: it creates a new document on first write and updates the existing one on subsequent writes.
