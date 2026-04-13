# ClickHouse Backend

The `ClickHouseCheckpointStore` persists checkpoints in a
[ClickHouse](https://clickhouse.com/) table using the
`ReplacingMergeTree` engine so that the latest version of each
`stream_id` is always surfaced efficiently.

## Installation

```bash
pip install clickhouse-driver
```

## Usage

```python
from clickhouse_driver import Client
from stream_checkpoint.backends.clickhouse_store import ClickHouseCheckpointStore
from stream_checkpoint.base import Checkpoint

client = Client(host="localhost", port=9000)

store = ClickHouseCheckpointStore(
    client=client,
    database="analytics",
    table="stream_checkpoints",
)

# Save a checkpoint
store.save(Checkpoint(stream_id="events", offset="1024", metadata={"shard": 3}))

# Load a checkpoint
cp = store.load("events")
print(cp.offset)  # "1024"

# List all tracked streams
print(store.list_streams())

# Delete a checkpoint
store.delete("events")
```

## Constructor Parameters

| Parameter  | Type   | Default         | Description                          |
|------------|--------|-----------------|--------------------------------------|
| `client`   | object | **required**    | A `clickhouse_driver.Client` instance |
| `database` | str    | `"default"`     | ClickHouse database name             |
| `table`    | str    | `"checkpoints"` | Table name for checkpoint storage    |

## Notes

- The table is created automatically on first use (`CREATE TABLE IF NOT EXISTS`).
- `ReplacingMergeTree` is used so that re-inserting a checkpoint for the
  same `stream_id` effectively updates it; queries use `FINAL` to read
  the deduplicated view.
- `DELETE` operations use `ALTER TABLE … DELETE` (mutation) which is
  eventually consistent in ClickHouse.
