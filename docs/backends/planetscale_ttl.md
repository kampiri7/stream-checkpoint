# PlanetScale TTL Backend

The `planetscale_ttl` backend stores checkpoints in a [PlanetScale](https://planetscale.com/) MySQL-compatible database with automatic expiry via a `expires_at` column.

## Installation

```bash
pip install PyMySQL
```

## Usage

```python
import pymysql
from stream_checkpoint.backends.planetscale_ttl_store import PlanetScaleTTLCheckpointStore

conn = pymysql.connect(
    host="<host>",
    user="<user>",
    password="<password>",
    database="<database>",
    ssl={"ssl": True},
)

store = PlanetScaleTTLCheckpointStore(
    connection=conn,
    table="checkpoints",
    ttl=3600,  # seconds until expiry
)
```

## Parameters

| Parameter    | Type   | Default         | Description                          |
|-------------|--------|-----------------|--------------------------------------|
| `connection` | object | **required**    | A PlanetScale / PyMySQL connection   |
| `table`      | str    | `"checkpoints"` | Table name to store checkpoints      |
| `ttl`        | int    | `3600`          | Seconds before a checkpoint expires  |

## Behaviour

- On `save`, the `expires_at` column is set to `now + ttl`.
- On `load`, only rows where `expires_at > now` are returned.
- `list_checkpoints()` returns all non-expired checkpoints.
- Expired rows are not automatically purged; use a scheduled job or PlanetScale's scheduled queries to clean them up.

## Schema

```sql
CREATE TABLE `checkpoints` (
    `pipeline_id` VARCHAR(255) NOT NULL,
    `offset`      TEXT         NOT NULL,
    `metadata`    TEXT,
    `timestamp`   DOUBLE       NOT NULL,
    `expires_at`  DOUBLE       NOT NULL,
    PRIMARY KEY (`pipeline_id`)
);
```
