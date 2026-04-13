# LevelDB Backend

The `LevelDBCheckpointStore` persists checkpoints in a local [LevelDB](https://github.com/google/leveldb) database using the [`plyvel`](https://plyvel.readthedocs.io/) Python binding.

## Installation

```bash
pip install plyvel
```

## Usage

```python
from stream_checkpoint.backends.leveldb_store import LevelDBCheckpointStore
from stream_checkpoint.base import Checkpoint
from datetime import datetime, timezone

store = LevelDBCheckpointStore(db_path="/var/data/checkpoints", prefix="ckpt:")

cp = Checkpoint(
    pipeline_id="my_pipeline",
    stream_id="topic_0",
    offset=1024,
    metadata={"partition": 0},
    timestamp=datetime.now(tz=timezone.utc),
)

store.save(cp)
loaded = store.load("my_pipeline", "topic_0")
print(loaded.offset)  # 1024

# List all checkpoints for a pipeline
all_cps = store.list_checkpoints("my_pipeline")

# Delete a checkpoint
store.delete("my_pipeline", "topic_0")

# Close the database when done
store.close()
```

## Constructor Parameters

| Parameter  | Type  | Default          | Description                                |
|------------|-------|------------------|--------------------------------------------|
| `db_path`  | `str` | *required*       | Filesystem path to the LevelDB directory.  |
| `prefix`   | `str` | `"checkpoint:"` | Prefix prepended to all stored keys.       |

## Notes

- LevelDB is a single-process, embedded key-value store — it is **not** suitable for concurrent access from multiple processes.
- Use this backend for local, single-node streaming pipelines where low-latency persistence is required without a network dependency.
- The database directory is created automatically if it does not exist.
