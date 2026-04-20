# CockroachDB TTL Backend

The `CockroachDBTTLCheckpointStore` stores checkpoints in a [CockroachDB](https://www.cockroachlabs.com/) table and leverages CockroachDB's native **row-level TTL** feature to automatically expire stale records.

## Installation

```bash
pip install psycopg2-binary  # or another CockroachDB-compatible driver
```

## Usage

```python
import psycopg2
from stream_checkpoint.backends.cockroachdb_ttl_store import CockroachDBTTLCheckpointStore

conn = psycopg2.connect(
    "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
)

store = CockroachDBTTLCheckpointStore(
    conn=conn,
    table="stream_checkpoints",
    ttl_seconds=86400,   # 24 hours
    prefix="myapp:",
)
```

## Parameters

| Parameter     | Type   | Default          | Description                                      |
|---------------|--------|------------------|--------------------------------------------------|
| `conn`        | object | —                | A live database connection (psycopg2-compatible) |
| `table`       | str    | `"checkpoints"`  | Table name to store checkpoints in               |
| `ttl_seconds` | int    | `86400`          | Seconds until a checkpoint row expires           |
| `prefix`      | str    | `""`             | Optional key prefix for namespacing              |

## Schema

The store automatically creates the table on first use:

```sql
CREATE TABLE IF NOT EXISTS checkpoints (
    key        TEXT PRIMARY KEY,
    data       TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '86400 seconds'
) WITH (ttl_expiration_expression = 'expires_at');
```

CockroachDB's TTL job will delete rows once `expires_at` passes, so no manual cleanup is required.

## Notes

- Row-level TTL requires CockroachDB **v22.2** or later.
- Each `save()` call refreshes the `expires_at` timestamp.
- `load()` filters on `expires_at > NOW()` to avoid returning logically-expired rows before the TTL job runs.
