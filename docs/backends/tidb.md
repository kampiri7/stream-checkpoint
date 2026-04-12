# TiDB Backend

The `TiDBCheckpointStore` persists checkpoints in a [TiDB](https://www.pingcap.com/tidb/) database, a MySQL-compatible distributed SQL database designed for horizontal scalability and high availability.

## Installation

Install the MySQL connector (TiDB is wire-compatible with MySQL):

```bash
pip install mysql-connector-python
```

## Usage

```python
import mysql.connector
from stream_checkpoint.backends.tidb_store import TiDBCheckpointStore
from stream_checkpoint.base import Checkpoint
from datetime import datetime

conn = mysql.connector.connect(
    host="localhost",
    port=4000,          # default TiDB port
    user="root",
    password="",
    database="mydb",
)

store = TiDBCheckpointStore(conn, table="checkpoints")

cp = Checkpoint(
    pipeline_id="etl-pipeline",
    stream_id="orders-topic",
    offset=1024,
    metadata={"batch": 7},
    updated_at=datetime.utcnow(),
)

store.save(cp)
loaded = store.load("etl-pipeline", "orders-topic")
print(loaded.offset)  # 1024
```

## Via Registry

```python
from stream_checkpoint.backends import get_backend

TiDBCheckpointStore = get_backend("tidb")
```

## Schema

The table is created automatically on first use:

| Column        | Type          | Notes                         |
|---------------|---------------|-------------------------------|
| `pipeline_id` | VARCHAR(255)  | Part of composite primary key |
| `stream_id`   | VARCHAR(255)  | Part of composite primary key |
| `offset`      | BIGINT        |                               |
| `metadata`    | TEXT          | JSON-encoded, nullable        |
| `updated_at`  | DATETIME      |                               |

## Parameters

| Parameter    | Type   | Default         | Description                          |
|--------------|--------|-----------------|--------------------------------------|
| `connection` | object | *required*      | PEP 249 database connection object   |
| `table`      | str    | `"checkpoints"` | Table name for checkpoint storage    |
