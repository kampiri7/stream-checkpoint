# Firestore TTL Backend

The `firestore_ttl` backend stores checkpoints in [Google Cloud Firestore](https://cloud.google.com/firestore) with optional document-level TTL support.

## Installation

```bash
pip install google-cloud-firestore
```

## Usage

```python
from google.cloud import firestore
from stream_checkpoint.backends.firestore_ttl_store import FirestoreTTLCheckpointStore
from stream_checkpoint.base import Checkpoint

client = firestore.Client(project="my-gcp-project")

store = FirestoreTTLCheckpointStore(
    client=client,
    collection="checkpoints",
    ttl_seconds=86400,  # 24 hours; set to None to disable TTL
)

cp = Checkpoint(pipeline_id="etl", stream_id="orders", offset=1024)
store.save(cp)

loaded = store.load("etl", "orders")
print(loaded.offset)  # 1024

store.delete("etl", "orders")
```

## TTL Configuration

When `ttl_seconds` is provided, each document is written with an `expires_at` timestamp field.  
You must configure a [Firestore TTL policy](https://cloud.google.com/firestore/docs/ttl) on the `expires_at` field in the Google Cloud Console or via `gcloud` to enable automatic expiry:

```bash
gcloud firestore fields ttls update expires_at \
  --collection-group=checkpoints \
  --enable-ttl
```

## Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `client` | `firestore.Client` | required | Authenticated Firestore client |
| `collection` | `str` | `"checkpoints"` | Firestore collection name |
| `ttl_seconds` | `int \| None` | `None` | Seconds until document expires; `None` disables TTL |

## Methods

- `save(checkpoint)` — Upserts a checkpoint document.
- `load(pipeline_id, stream_id)` — Returns a `Checkpoint` or `None`.
- `delete(pipeline_id, stream_id)` — Removes the checkpoint document.
- `list_checkpoints(pipeline_id)` — Returns all checkpoints for a pipeline.
