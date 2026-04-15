"""Neo4j checkpoint store backend."""

import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class Neo4jCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Neo4j graph database.

    Each checkpoint is stored as a node with label ``Checkpoint``
    and a unique ``checkpoint_id`` property.

    Args:
        driver: A ``neo4j.GraphDatabase`` driver instance.
        database: Neo4j database name (default ``"neo4j"``).
    """

    def __init__(self, driver, database: str = "neo4j") -> None:
        self._driver = driver
        self._database = database
        self._ensure_constraint()

    def _ensure_constraint(self) -> None:
        """Create a uniqueness constraint on checkpoint_id if not present."""
        with self._driver.session(database=self._database) as session:
            session.run(
                "CREATE CONSTRAINT checkpoint_id_unique IF NOT EXISTS "
                "FOR (c:Checkpoint) REQUIRE c.checkpoint_id IS UNIQUE"
            )

    def save(self, checkpoint: Checkpoint) -> None:
        data = json.dumps(checkpoint.to_dict())
        with self._driver.session(database=self._database) as session:
            session.run(
                "MERGE (c:Checkpoint {checkpoint_id: $cid}) "
                "SET c.data = $data",
                cid=checkpoint.checkpoint_id,
                data=data,
            )

    def load(self, checkpoint_id: str) -> Optional[Checkpoint]:
        with self._driver.session(database=self._database) as session:
            result = session.run(
                "MATCH (c:Checkpoint {checkpoint_id: $cid}) RETURN c.data AS data",
                cid=checkpoint_id,
            )
            record = result.single()
            if record is None:
                return None
            return Checkpoint.from_dict(json.loads(record["data"]))

    def delete(self, checkpoint_id: str) -> None:
        with self._driver.session(database=self._database) as session:
            session.run(
                "MATCH (c:Checkpoint {checkpoint_id: $cid}) DETACH DELETE c",
                cid=checkpoint_id,
            )

    def list_checkpoints(self) -> list:
        with self._driver.session(database=self._database) as session:
            result = session.run(
                "MATCH (c:Checkpoint) RETURN c.data AS data ORDER BY c.checkpoint_id"
            )
            return [Checkpoint.from_dict(json.loads(r["data"])) for r in result]

    def clear(self) -> None:
        with self._driver.session(database=self._database) as session:
            session.run("MATCH (c:Checkpoint) DETACH DELETE c")
