# Google Cloud Spanner Backend

The `SpannerCheckpointStore` stores checkpoints in a [Google Cloud Spanner](https://cloud.google.com/spanner) table, providing globally distributed, strongly consistent storage.

## Installation

Install the required dependency:

```bash
pip install google-cloud-spanner
```

## Usage

```python
from stream_checkpoint.backends.spanner_store import SpannerCheckpointStore
from stream_checkpoint.base import Checkpoint
from datetime import datetime, timezone

store = SpannerCheckpointStore(
    instance_id="my-instance",
    database_id="my-database",
    table="checkpoints",  # optional, default: "checkpoints"
)

cp = Checkpoint(
    pipeline_id="my-pipeline",
    stream_id="topic-0",
    offset="42",
    metadata={"consumer_group": "grp1"},
    updated_at=datetime.now(timezone.utc),
)

store.save(cp)
loaded = store.load("my-pipeline", "topic-0")
print(loaded.offset)  # "42"

store.delete("my-pipeline", "topic-0")
```

## Schema

The table is created automatically on first use with the following DDL:

```sql
CREATE TABLE IF NOT EXISTS checkpoints (
  pipeline_id STRING(256) NOT NULL,
  stream_id   STRING(256) NOT NULL,
  offset      STRING(MAX) NOT NULL,
  metadata    STRING(MAX),
  updated_at  TIMESTAMP  NOT NULL
) PRIMARY KEY (pipeline_id, stream_id)
```

## Configuration

| Parameter     | Type   | Default         | Description                        |
|---------------|--------|-----------------|------------------------------------|
| `instance_id` | `str`  | required        | Spanner instance ID                |
| `database_id` | `str`  | required        | Spanner database ID                |
| `table`       | `str`  | `"checkpoints"` | Table name for storing checkpoints |
| `client`      | object | `None`          | Custom `spanner.Client` (testing)  |

## Notes

- The `client` parameter is intended for testing with a fake/mock client.
- Metadata is serialized as JSON and stored in a `STRING(MAX)` column.
- `updated_at` is stored as a Spanner `TIMESTAMP`.
