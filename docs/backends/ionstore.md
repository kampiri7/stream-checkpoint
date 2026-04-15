# IonStore Backend

The `ionstore` backend stores checkpoints in an **Amazon S3** bucket using
JSON serialization (Ion-compatible format). It is structurally identical to
the `s3` backend but uses a distinct key suffix (`.ion.json`) and supports
bulk listing of checkpoints per pipeline.

## Installation

```bash
pip install boto3
```

## Usage

```python
import boto3
from stream_checkpoint.backends.ionstore_store import IonStoreCheckpointStore
from stream_checkpoint.base import Checkpoint

client = boto3.client("s3", region_name="us-east-1")
store = IonStoreCheckpointStore(client, bucket="my-bucket", prefix="checkpoints/")

cp = Checkpoint(pipeline_id="etl-pipeline", stream_id="topic-A", offset=1024)
store.save(cp)

loaded = store.load("etl-pipeline", "topic-A")
print(loaded.offset)  # 1024

# List all checkpoints for a pipeline
all_cp = store.list_checkpoints("etl-pipeline")
for c in all_cp:
    print(c.stream_id, c.offset)

# Delete a checkpoint
store.delete("etl-pipeline", "topic-A")
```

## Configuration

| Parameter | Type   | Default          | Description                        |
|-----------|--------|------------------|------------------------------------|
| `client`  | object | —                | boto3 S3 client instance           |
| `bucket`  | str    | —                | Target S3 bucket name              |
| `prefix`  | str    | `checkpoints/`   | Key prefix for checkpoint objects  |

## Key Format

Keys are stored as:

```
{prefix}{pipeline_id}/{stream_id}.ion.json
```

## Notes

- The `.ion.json` suffix distinguishes IonStore objects from plain S3 checkpoints.
- `list_checkpoints(pipeline_id)` fetches all checkpoints under a pipeline prefix.
- Serialization uses standard `json` for broad compatibility.
