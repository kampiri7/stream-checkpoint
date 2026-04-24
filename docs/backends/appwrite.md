# Appwrite Backend

The `AppwriteCheckpointStore` persists checkpoints in an [Appwrite](https://appwrite.io/) database collection.

## Installation

```bash
pip install appwrite
```

## Usage

```python
from appwrite.client import Client
from appwrite.services.databases import Databases
from stream_checkpoint.backends.appwrite_store import AppwriteCheckpointStore
from stream_checkpoint.base import Checkpoint

client = Client()
client.set_endpoint("https://cloud.appwrite.io/v1")
client.set_project("<PROJECT_ID>")
client.set_key("<API_KEY>")

databases = Databases(client)

store = AppwriteCheckpointStore(
    client=databases,
    database_id="my-database",
    collection_id="checkpoints",
    prefix="ckpt",
)

# Save a checkpoint
cp = Checkpoint(pipeline_id="my-pipeline", key="offset", value="42")
store.save(cp)

# Load a checkpoint
loaded = store.load("my-pipeline", "offset")
print(loaded.value)  # "42"

# Delete a checkpoint
store.delete("my-pipeline", "offset")

# List all checkpoints for a pipeline
checkpoints = store.list_checkpoints("my-pipeline")
```

## Constructor Parameters

| Parameter | Type | Description |
|---|---|---|
| `client` | `Databases` | Appwrite `Databases` service instance |
| `database_id` | `str` | Target Appwrite database ID |
| `collection_id` | `str` | Target Appwrite collection ID |
| `prefix` | `str` | Optional document ID prefix (default: `"checkpoint"`) |

## Collection Schema

Create your Appwrite collection with at least the following string attributes:
`pipeline_id`, `key`, `value`, `payload`, and optionally `timestamp`.

## Notes

- Document IDs are derived as `{prefix}_{pipeline_id}_{key}`.
- `payload` is stored as a JSON string to accommodate arbitrary nested data.
- Upsert behaviour is emulated via a get-then-create/update pattern.
