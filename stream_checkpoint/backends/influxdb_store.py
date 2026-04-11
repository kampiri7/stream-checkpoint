import json
from datetime import datetime, timezone
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class InfluxDBCheckpointStore(BaseCheckpointStore):
    """
    Checkpoint store backed by InfluxDB.

    Checkpoints are stored as points in a measurement. The latest point
    for a given (pipeline_id, stream_id) pair is used on load.
    """

    def __init__(
        self,
        client,
        bucket: str,
        org: str,
        measurement: str = "stream_checkpoint",
    ):
        self._client = client
        self._bucket = bucket
        self._org = org
        self._measurement = measurement
        self._write_api = client.write_api()
        self._query_api = client.query_api()

    def _tags(self, pipeline_id: str, stream_id: str) -> str:
        return f'pipeline_id="{pipeline_id}",stream_id="{stream_id}"'

    def save(self, checkpoint: Checkpoint) -> None:
        payload = json.dumps(checkpoint.to_dict())
        record = (
            f'{self._measurement},pipeline_id={checkpoint.pipeline_id}'
            f',stream_id={checkpoint.stream_id} '
            f'data="{payload.replace(chr(34), chr(92) + chr(34))}""
        )
        self._write_api.write(bucket=self._bucket, org=self._org, record=record)

    def load(
        self, pipeline_id: str, stream_id: str
    ) -> Optional[Checkpoint]:
        flux = (
            f'from(bucket: "{self._bucket}")'
            f' |> range(start: 0)'
            f' |> filter(fn: (r) => r._measurement == "{self._measurement}")'
            f' |> filter(fn: (r) => r.pipeline_id == "{pipeline_id}")'
            f' |> filter(fn: (r) => r.stream_id == "{stream_id}")'
            f' |> last()'
        )
        tables = self._query_api.query(flux, org=self._org)
        for table in tables:
            for record in table.records:
                raw = record.get_value()
                data = json.loads(raw)
                return Checkpoint.from_dict(data)
        return None

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        start = "1970-01-01T00:00:00Z"
        stop = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        predicate = (
            f'_measurement="{self._measurement}" '
            f'AND pipeline_id="{pipeline_id}" '
            f'AND stream_id="{stream_id}"'
        )
        delete_api = self._client.delete_api()
        delete_api.delete(
            start, stop, predicate, bucket=self._bucket, org=self._org
        )

    def list_checkpoints(self, pipeline_id: str):
        flux = (
            f'from(bucket: "{self._bucket}")'
            f' |> range(start: 0)'
            f' |> filter(fn: (r) => r._measurement == "{self._measurement}")'
            f' |> filter(fn: (r) => r.pipeline_id == "{pipeline_id}")'
            f' |> last()'
        )
        tables = self._query_api.query(flux, org=self._org)
        results = []
        for table in tables:
            for record in table.records:
                raw = record.get_value()
                data = json.loads(raw)
                results.append(Checkpoint.from_dict(data))
        return results
