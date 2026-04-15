# Neo4j Backend

The `Neo4jCheckpointStore` stores checkpoints as nodes in a [Neo4j](https://neo4j.com/) graph database.

## Installation

```bash
pip install neo4j
```

## Usage

```python
from neo4j import GraphDatabase
from stream_checkpoint.backends.neo4j_store import Neo4jCheckpointStore
from stream_checkpoint.base import Checkpoint

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
store = Neo4jCheckpointStore(driver, database="neo4j")

cp = Checkpoint(checkpoint_id="job-1", stream_id="events", offset=1024)
store.save(cp)

loaded = store.load("job-1")
print(loaded.offset)  # 1024

store.delete("job-1")
```

## Constructor Parameters

| Parameter  | Type   | Default    | Description                          |
|------------|--------|------------|--------------------------------------|
| `driver`   | object | *required* | A `neo4j.GraphDatabase` driver.      |
| `database` | str    | `"neo4j"`  | Name of the Neo4j database to use.   |

## Data Model

Each checkpoint is stored as a `(:Checkpoint)` node with two properties:

- `checkpoint_id` — unique identifier (constrained).
- `data` — JSON-serialised checkpoint payload.

## Methods

| Method                      | Description                              |
|-----------------------------|------------------------------------------|
| `save(checkpoint)`          | Upsert a checkpoint node.                |
| `load(checkpoint_id)`       | Return a `Checkpoint` or `None`.         |
| `delete(checkpoint_id)`     | Remove a checkpoint node.                |
| `list_checkpoints()`        | Return all checkpoints ordered by ID.    |
| `clear()`                   | Delete all checkpoint nodes.             |

## Notes

- A uniqueness constraint on `checkpoint_id` is created automatically on first use.
- Requires Neo4j 4.x or later for `IF NOT EXISTS` constraint syntax.
