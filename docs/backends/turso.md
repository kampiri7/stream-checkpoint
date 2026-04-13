# Turso (libSQL) Backend

The `TursoCheckpointStore` backend stores checkpoints in a [Turso](https://turso.tech/) database using the `libsql-client` Python SDK.

## Installation

```bash
pip install libsql-client
```

## Usage

```python
import libsql_client
from stream_checkpoint.backends.turso_store import TursoCheckpointStore
from stream_checkpoint.base import Checkpoint

client = libsql_client.create_client(
    url="libsql://your-database.turso.io",
    auth_token="your-auth-token",
)

store = TursoCheckpointStore(client=client, table="checkpoints")

cp = Checkpoint(stream_id="my-stream", partition="0", offset="1024")
store.save(cp)

loaded = store.load("my-stream", "0")
print(loaded.offset)  # "1024"
```

## Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `client` | `libsql_client.Client` | required | Authenticated libsql client instance |
| `table` | `str` | `"checkpoints"` | Table name used to store checkpoints |

## Schema

The store automatically creates the following table if it does not exist:

```sql
CREATE TABLE IF NOT EXISTS checkpoints (
    stream_id   TEXT NOT NULL,
    partition   TEXT NOT NULL,
    offset      TEXT NOT NULL,
    metadata    TEXT,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (stream_id, partition)
);
```

## Methods

- **`save(checkpoint)`** — Upserts a checkpoint record.
- **`load(stream_id, partition)`** — Returns a `Checkpoint` or `None`.
- **`delete(stream_id, partition)`** — Removes a checkpoint record.
- **`list_checkpoints(stream_id)`** — Returns all checkpoints for a stream.

## Notes

- Turso is a SQLite-compatible edge database. The store uses standard SQL with `?` placeholders compatible with libsql-client.
- For local development you can use an in-memory or file-based libSQL database without a Turso account.
