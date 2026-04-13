# ArangoDB Backend

The `ArangoDBCheckpointStore` persists checkpoints in an [ArangoDB](https://www.arangodb.com/) collection.

## Installation

```bash
pip install python-arango
```

## Usage

```python
from arango import ArangoClient
from stream_checkpoint.backends.arangodb_store import ArangoDBCheckpointStore

client = ArangoClient(hosts="http://localhost:8529")
store = ArangoDBCheckpointStore(
    client=client,
    database="my_database",
    collection="checkpoints",  # created automatically if absent
)
```

## Constructor Parameters

| Parameter    | Type           | Default         | Description                                     |
|-------------|----------------|-----------------|-------------------------------------------------|
| `client`    | `ArangoClient` | **required**    | An authenticated `python-arango` client.        |
| `database`  | `str`          | **required**    | Name of the ArangoDB database.                  |
| `collection`| `str`          | `"checkpoints"` | Collection used to store checkpoint documents.  |

## Behaviour

- Documents are keyed by `"{pipeline_id}::{stream_id}"` using ArangoDB's `_key` field.
- `save()` uses `insert(..., overwrite=True)` so repeated saves are idempotent.
- `delete()` silently ignores missing documents (`ignore_missing=True`).
- `list_checkpoints(pipeline_id)` returns all checkpoints for a given pipeline.
- `exists(pipeline_id, stream_id)` returns `True` if a checkpoint is present.

## Registering via the backend registry

```python
from stream_checkpoint.backends import get_backend

ArangoDBCheckpointStore = get_backend("arangodb")
```
