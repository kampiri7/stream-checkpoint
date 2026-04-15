# VoltDB Backend

The `VoltDBCheckpointStore` persists checkpoints in a [VoltDB](https://www.voltdb.com/) table.
VoltDB is an in-memory, ACID-compliant NewSQL database optimised for high-throughput streaming workloads.

## Installation

Install the official VoltDB Python client:

```bash
pip install voltdb-client
```

## Usage

```python
import voltdb
from stream_checkpoint.backends.voltdb_store import VoltDBCheckpointStore
from stream_checkpoint.base import Checkpoint
from datetime import datetime, timezone

client = voltdb.connect(host="localhost", port=21212)
store = VoltDBCheckpointStore(client, table="checkpoints")

cp = Checkpoint(
    pipeline_id="my_pipeline",
    stream_id="topic-0",
    offset={"offset": 1024},
    updated_at=datetime.now(timezone.utc),
)

store.save(cp)
loaded = store.load("my_pipeline", "topic-0")
print(loaded.offset)  # {'offset': 1024}

store.delete("my_pipeline", "topic-0")
```

## Constructor Parameters

| Parameter | Type   | Default         | Description                          |
|-----------|--------|-----------------|--------------------------------------|
| `client`  | object | *required*      | VoltDB client instance               |
| `table`   | `str`  | `"checkpoints"` | Table name used to store checkpoints |

## Schema

The table is created automatically on first use:

```sql
CREATE TABLE IF NOT EXISTS checkpoints (
    pipeline_id VARCHAR(255) NOT NULL,
    stream_id   VARCHAR(255) NOT NULL,
    offset      VARCHAR(4096) NOT NULL,
    metadata    VARCHAR(4096),
    updated_at  TIMESTAMP NOT NULL,
    PRIMARY KEY (pipeline_id, stream_id)
);
```

## Notes

- `offset` and `metadata` are stored as JSON strings.
- `UPSERT INTO` ensures idempotent saves.
- The backend is well-suited for low-latency checkpoint updates in high-frequency pipelines.
