# stream-checkpoint

Lightweight library for managing checkpoints in streaming data pipelines with pluggable storage backends.

## Installation

```bash
pip install stream-checkpoint
```

## Usage

```python
from stream_checkpoint import CheckpointManager
from stream_checkpoint.backends import RedisBackend

# Initialize with a storage backend
backend = RedisBackend(host="localhost", port=6379)
manager = CheckpointManager(backend=backend, pipeline_id="my-pipeline")

# Save a checkpoint
manager.save(offset=1024, metadata={"topic": "events", "partition": 0})

# Resume from the last checkpoint
checkpoint = manager.load()
print(f"Resuming from offset: {checkpoint.offset}")

# Use as a context manager for automatic checkpointing
with manager.track() as cp:
    for record in stream:
        process(record)
        cp.update(offset=record.offset)
```

### Built-in Backends

| Backend | Install Extra |
|---------|--------------|
| Redis   | `pip install stream-checkpoint[redis]` |
| S3      | `pip install stream-checkpoint[s3]` |
| SQLite  | included by default |

### Custom Backend

```python
from stream_checkpoint.backends import BaseBackend

class MyBackend(BaseBackend):
    def save(self, key, data): ...
    def load(self, key): ...
```

## License

MIT © [stream-checkpoint contributors](LICENSE)