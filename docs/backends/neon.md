# Neon Backend

The `NeonCheckpointStore` persists checkpoints in a [Neon](https://neon.tech) serverless
Postgres database.

## Installation

Install the required driver:

```bash
pip install psycopg2-binary
```

## Usage

```python
from stream_checkpoint.backends.neon_store import NeonCheckpointStore
from stream_checkpoint.base import Checkpoint

store = NeonCheckpointStore(
    connection_string="postgresql://user:password@ep-xyz.us-east-2.aws.neon.tech/neondb",
    table="checkpoints",
)

# Save a checkpoint
cp = Checkpoint(stream_id="my-stream", partition_id="0", offset="1024")
store.save(cp)

# Load a checkpoint
cp = store.load("my-stream", "0")
print(cp.offset)  # "1024"

# List all checkpoints for a stream
all_cp = store.list_checkpoints("my-stream")

# Delete a checkpoint
store.delete("my-stream", "0")

# Close the connection when done
store.close()
```

## Configuration

| Parameter           | Type  | Default         | Description                          |
|---------------------|-------|-----------------|--------------------------------------|
| `connection_string` | `str` | **required**    | Neon/Postgres connection URI         |
| `table`             | `str` | `"checkpoints"` | Table name to store checkpoints in   |

## Notes

- The table is created automatically on first use.
- Uses `ON CONFLICT ... DO UPDATE` for atomic upserts.
- `autocommit` is enabled on the connection to avoid manual transaction management.
- Compatible with any standard Postgres-compatible connection string.
