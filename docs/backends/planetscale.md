# PlanetScale Backend

The `PlanetScaleCheckpointStore` stores checkpoints in a [PlanetScale](https://planetscale.com/) serverless MySQL-compatible database.

## Requirements

Install a MySQL-compatible Python client:

```bash
pip install PyMySQL
# or
pip install mysql-connector-python
```

## Usage

```python
import pymysql
from stream_checkpoint.backends.planetscale_store import PlanetScaleCheckpointStore
from stream_checkpoint.base import Checkpoint

conn = pymysql.connect(
    host="aws.connect.psdb.cloud",
    user="your_username",
    password="your_password",
    database="your_database",
    ssl={"ca": "/etc/ssl/cert.pem"},
)

store = PlanetScaleCheckpointStore(client=conn, database="your_database", table="checkpoints")

# Save a checkpoint
cp = Checkpoint(stream_id="orders", partition="p0", offset=1024, metadata={"source": "kafka"})
store.save(cp)

# Load a checkpoint
loaded = store.load("orders", "p0")
print(loaded.offset)  # 1024

# List all checkpoints for a stream
all_cps = store.list_checkpoints("orders")

# Delete a checkpoint
store.delete("orders", "p0")
```

## Constructor Parameters

| Parameter  | Type   | Default         | Description                              |
|------------|--------|-----------------|------------------------------------------|
| `client`   | object | required        | MySQL-compatible connection object       |
| `database` | str    | required        | PlanetScale database name                |
| `table`    | str    | `"checkpoints"` | Table name for storing checkpoints       |

## Schema

The store automatically creates the following table if it does not exist:

```sql
CREATE TABLE IF NOT EXISTS `checkpoints` (
    `stream_id`  VARCHAR(255) NOT NULL,
    `partition`  VARCHAR(255) NOT NULL,
    `offset`     BIGINT NOT NULL,
    `metadata`   JSON,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`stream_id`, `partition`)
);
```

## Notes

- PlanetScale uses a MySQL-compatible wire protocol, so any standard MySQL Python driver works.
- The `metadata` field is stored as JSON and deserialized automatically on load.
- `save()` performs an upsert using `INSERT ... ON DUPLICATE KEY UPDATE`.
