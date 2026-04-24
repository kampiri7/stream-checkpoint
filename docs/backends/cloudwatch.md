# CloudWatch Backend

The `cloudwatch` backend stores checkpoints as log events in **AWS CloudWatch Logs**.
Each `(stream_id, partition_key)` pair maps to a dedicated log stream within a
configured log group. The most recent log event in the stream represents the
current checkpoint.

## Installation

```bash
pip install boto3
```

## Usage

```python
import boto3
from stream_checkpoint.backends.cloudwatch_store import CloudWatchCheckpointStore
from stream_checkpoint.base import Checkpoint

client = boto3.client("logs", region_name="us-east-1")
store = CloudWatchCheckpointStore(
    client=client,
    log_group="/my-app/checkpoints",
    prefix="checkpoint",
)

cp = Checkpoint(stream_id="orders", partition_key="shard-0", offset="1024", metadata={})
store.save(cp)

loaded = store.load("orders", "shard-0")
print(loaded.offset)  # "1024"
```

## Constructor Parameters

| Parameter    | Type   | Description                                         |
|--------------|--------|-----------------------------------------------------|
| `client`     | object | A `boto3` CloudWatch Logs client.                   |
| `log_group`  | str    | Name of the CloudWatch log group.                   |
| `prefix`     | str    | Prefix for log stream names (default `checkpoint`). |

## Behaviour

- **save** – Appends a JSON-encoded checkpoint as a log event to the log stream
  `{prefix}/{stream_id}/{partition_key}`.  The log group and stream are created
  automatically if they do not exist.
- **load** – Retrieves the last log event from the stream and deserialises it.
  Returns `None` if the stream does not exist or is empty.
- **delete** – Deletes the entire log stream, removing all history for that key.
- **list_checkpoints** – Enumerates all log streams under `{prefix}/{stream_id}/`
  and returns the latest checkpoint from each.

## Notes

- CloudWatch Logs is an append-only store; old checkpoint events accumulate over
  time.  Consider setting a log retention policy on the log group to manage costs.
- The `sequenceToken` required by `put_log_events` is tracked in memory per
  process; concurrent writers to the same stream may encounter sequence-token
  conflicts.
