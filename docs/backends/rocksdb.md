# RocksDB Backend

The `rocksdb` backend persists checkpoints using an embedded
[RocksDB](https://rocksdb.org/) database via the
[`python-rocksdb`](https://pypi.org/project/python-rocksdb/) package.

## Installation

```bash
pip install python-rocksdb
```

> **Note:** `python-rocksdb` requires the native RocksDB shared library to be
> installed on your system (e.g. `librocksdb-dev` on Debian/Ubuntu).

## Usage

```python
from stream_checkpoint.backends.rocksdb_store import RocksDBCheckpointStore
from stream_checkpoint.base import Checkpoint

store = RocksDBCheckpointStore(db_path="/var/data/checkpoints")

cp = Checkpoint(pipeline_id="my_pipeline", stream_id="topic-0", offset=1024)
store.save(cp)

loaded = store.load("my_pipeline", "topic-0")
print(loaded.offset)  # 1024

store.delete("my_pipeline", "topic-0")
store.close()
```

## Constructor Parameters

| Parameter  | Type  | Default          | Description                              |
|------------|-------|------------------|------------------------------------------|
| `db_path`  | `str` | **required**     | Filesystem path for the RocksDB database |
| `prefix`   | `str` | `"checkpoint:"` | Prefix prepended to every stored key     |

## Key Format

Keys are stored as UTF-8 bytes with the pattern:

```
<prefix><pipeline_id>:<stream_id>
```

## Notes

- The database is opened with `create_if_missing=True`, so the path will be
  created automatically on first use.
- Call `store.close()` when you are done to release the native RocksDB handle.
- `list_checkpoints` uses a prefix scan and relies on lexicographic ordering
  of keys, so choose a prefix that does not clash with other data stored in
  the same database.
