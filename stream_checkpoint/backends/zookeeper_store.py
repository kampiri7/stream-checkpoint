import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class ZookeeperCheckpointStore(BaseCheckpointStore):
    """
    Checkpoint store backed by Apache ZooKeeper.

    Requires the ``kazoo`` package::

        pip install kazoo

    Args:
        hosts: ZooKeeper connection string, e.g. ``"localhost:2181"``.
        root: Root znode path under which checkpoints are stored.
        timeout: Connection timeout in seconds.
    """

    def __init__(
        self,
        hosts: str = "localhost:2181",
        root: str = "/stream_checkpoint",
        timeout: float = 10.0,
    ) -> None:
        from kazoo.client import KazooClient

        self._root = root.rstrip("/")
        self._client = KazooClient(hosts=hosts, timeout=timeout)
        self._client.start()
        self._client.ensure_path(self._root)

    def _key(self, pipeline_id: str, stream_id: str) -> str:
        return f"{self._root}/{pipeline_id}/{stream_id}"

    def save(self, checkpoint: Checkpoint) -> None:
        path = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        data = json.dumps(checkpoint.to_dict()).encode("utf-8")
        if self._client.exists(path):
            self._client.set(path, data)
        else:
            self._client.ensure_path(path.rsplit("/", 1)[0])
            self._client.create(path, data)

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        path = self._key(pipeline_id, stream_id)
        if not self._client.exists(path):
            return None
        raw, _ = self._client.get(path)
        return Checkpoint.from_dict(json.loads(raw.decode("utf-8")))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        path = self._key(pipeline_id, stream_id)
        if self._client.exists(path):
            self._client.delete(path)

    def list_checkpoints(self, pipeline_id: str):
        path = f"{self._root}/{pipeline_id}"
        if not self._client.exists(path):
            return []
        children = self._client.get_children(path)
        results = []
        for stream_id in children:
            cp = self.load(pipeline_id, stream_id)
            if cp is not None:
                results.append(cp)
        return results

    def close(self) -> None:
        self._client.stop()
        self._client.close()
