import json
import os
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class FileCheckpointStore(BaseCheckpointStore):
    """File-based checkpoint store that persists checkpoints as JSON files."""

    def __init__(self, directory: str) -> None:
        """
        Initialize the FileCheckpointStore.

        Args:
            directory: Path to the directory where checkpoint files will be stored.
        """
        self.directory = directory
        os.makedirs(self.directory, exist_ok=True)

    def _path(self, stream_id: str) -> str:
        """Return the file path for a given stream_id."""
        safe_name = stream_id.replace(os.sep, "_")
        return os.path.join(self.directory, f"{safe_name}.json")

    def save(self, checkpoint: Checkpoint) -> None:
        """Persist a checkpoint to a JSON file."""
        path = self._path(checkpoint.stream_id)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(checkpoint.to_dict(), fh)

    def load(self, stream_id: str) -> Optional[Checkpoint]:
        """Load a checkpoint from disk, returning None if it does not exist."""
        path = self._path(stream_id)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return Checkpoint.from_dict(data)

    def delete(self, stream_id: str) -> None:
        """Delete the checkpoint file for the given stream_id if it exists."""
        path = self._path(stream_id)
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    def list_streams(self):
        """Return a list of stream IDs that have stored checkpoints."""
        streams = []
        for filename in os.listdir(self.directory):
            if filename.endswith(".json"):
                streams.append(filename[:-5])
        return streams
