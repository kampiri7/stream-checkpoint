"""Base classes and interfaces for stream-checkpoint storage backends."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class Checkpoint:
    """Represents a checkpoint in a streaming pipeline."""

    stream_id: str
    offset: Any
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize checkpoint to a dictionary."""
        return {
            "stream_id": self.stream_id,
            "offset": self.offset,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Checkpoint":
        """Deserialize checkpoint from a dictionary."""
        return cls(
            stream_id=data["stream_id"],
            offset=data["offset"],
            metadata=data.get("metadata", {}),
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
        )


class BaseCheckpointStore(ABC):
    """Abstract base class for checkpoint storage backends."""

    @abstractmethod
    def save(self, checkpoint: Checkpoint) -> None:
        """Persist a checkpoint for the given stream."""
        ...

    @abstractmethod
    def load(self, stream_id: str) -> Optional[Checkpoint]:
        """Retrieve the latest checkpoint for a stream. Returns None if not found."""
        ...

    @abstractmethod
    def delete(self, stream_id: str) -> bool:
        """Delete the checkpoint for a stream. Returns True if deleted, False if not found."""
        ...

    @abstractmethod
    def exists(self, stream_id: str) -> bool:
        """Check whether a checkpoint exists for the given stream."""
        ...

    def update_offset(self, stream_id: str, offset: Any) -> Optional[Checkpoint]:
        """Update only the offset of an existing checkpoint.

        Loads the current checkpoint for the stream, updates its offset and
        ``updated_at`` timestamp, persists it, and returns the updated
        checkpoint.  Returns ``None`` if no checkpoint exists for the stream.
        """
        checkpoint = self.load(stream_id)
        if checkpoint is None:
            return None
        checkpoint.offset = offset
        checkpoint.updated_at = datetime.utcnow()
        self.save(checkpoint)
        return checkpoint
