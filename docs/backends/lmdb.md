# LMDB Backend

The **LMDB** backend uses [Lightning Memory-Mapped Database](http://www.lmdb.tech/doc/) to store checkpoints on the local filesystem with extremely fast read and write performance.

## Installation

```bash
pip install lmdb
```

## Usage

```python
from stream_checkpoint.backends.lmdb_store import LMDBCheckpointStore
from stream_checkpoint.base import Checkpoint
from datetime import datetime, timezone

store = LMDBCheckpointStore(
    path="./my_checkpoints.lmdb",
    map_size=50 * 1024 * 1024,  # 50 MB
    namespace="myapp",
)

cp = Checkpoint(
    stream_id="orders-stream",
    pipeline_id="etl-pipeline",
    offset=1024,
    metadata={"partition": 0},
    timestamp=datetime.now(tz=timezone.utc),
)

store.save(cp)
loaded = store.load("orders-stream", "etl-pipeline")
print(loaded.offset)  # 1024

all_checkpoints = store.list_checkpoints("etl-pipeline")
store.delete("orders-stream", "etl-pipeline")
store.close()
```

## Constructor Parameters

| Parameter   | Type  | Default                  | Description                              |
|-------------|-------|--------------------------|------------------------------------------|
| `path`      | `str` | `"./checkpoints.lmdb"`   | Filesystem path for the LMDB environment |
| `map_size`  | `int` | `10485760` (10 MB)       | Maximum database size in bytes           |
| `namespace` | `str` | `"checkpoint"`           | Key prefix to namespace stored entries   |
| `client`    | any   | `None`                   | Pre-created LMDB environment (testing)   |

## Key Format

Keys are stored as `{namespace}:{pipeline_id}:{stream_id}`.

## Notes

- LMDB is a single-writer, multiple-reader database. Concurrent writes from multiple processes require careful coordination.
- The `map_size` must be set large enough to hold all checkpoint data; LMDB does not grow automatically on all platforms.
- Call `store.close()` when the store is no longer needed to release the memory-mapped file handle.
