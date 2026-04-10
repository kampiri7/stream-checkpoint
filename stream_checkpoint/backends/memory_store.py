"""In-memory checkpoint store backend."""

from typing import Dict, Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class MemoryCheckpointStore(BaseCheckpointStore):
    """A simple in-memory checkpoint store.

    Useful for testing, development, or short-lived pipelines
    where persistence across restarts is not required.
    """

    def __init__(self) -> None:
        """Initialize the in-memory store with an empty dict."""
        self._store: Dict[str, Dict] = {}

    def save(self, checkpoint: Checkpoint) -> None:
        """Save a checkpoint to memory.

        Args:
            checkpoint: The checkpoint to save.
        """
        self._store[checkpoint.stream_id] = checkpoint.to_dict()

    def load(self, stream_id: str) -> Optional[Checkpoint]:
        """Load a checkpoint from memory.

        Args:
            stream_id: The stream identifier to load the checkpoint for.

        Returns:
            The checkpoint if found, otherwise None.
        """
        data = self._store.get(stream_id)
        if data is None:
            return None
        return Checkpoint.from_dict(data)

    def delete(self, stream_id: str) -> None:
        """Delete a checkpoint from memory.

        Args:
            stream_id: The stream identifier whose checkpoint should be deleted.
        """
        self._store.pop(stream_id, None)

    def exists(self, stream_id: str) -> bool:
        """Check whether a checkpoint exists in memory.

        Args:
            stream_id: The stream identifier to check.

        Returns:
            True if the checkpoint exists, False otherwise.
        """
        return stream_id in self._store

    def clear(self) -> None:
        """Remove all checkpoints from the store."""
        self._store.clear()

    def __len__(self) -> int:
        """Return the number of checkpoints currently stored."""
        return len(self._store)
