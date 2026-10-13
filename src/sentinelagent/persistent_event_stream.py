"""SQLite-backed event storage for SentinelAgent.

Events are persisted as JSON and retrieved in publication order.
Completed events remain available for audit history.
Interrupted batches are returned to the pending queue on restart.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Callable


class PersistentEventStream:
    """Store events durably and support batch restoration."""

    def __init__(
        self,
        database_path: str | Path,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.database_path = str(database_path)
        self._clock = clock
        self._active_batches: dict[int, str] = {}

        if self.database_path != ":memory:":
            Path(self.database_path).parent.mkdir(
                parents=True, exist_ok=True
            )

        self._connection = sqlite3.connect(self.database_path)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")

        try:
            # Validate an existing event table before creating tables or
            # performing startup recovery that could modify existing data.
            self._validate_existing_schema()
            self._create_tables()
            self._recover_in_flight_batches()
        except Exception:
            self._connection.close()
            raise

    def _validate_existing_schema(self) -> None:
        """Reject an incompatible existing events table without modifying it."""
        row = self._connection.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'table' AND name = 'events'
            """
        ).fetchone()

        # A new database has no events table yet.
        if row is None:
            return

        schema_sql = row["sql"]
        if schema_sql is None or "'completed'" not in schema_sql.lower():
            raise RuntimeError(
                "incompatible database schema: events table must support "
                "the 'completed' status; back up the database and migrate "
                "its schema before reopening it"
            )

    def _create_tables(self) -> None:
        """Create required tables without running recovery operations."""
        with self._connection:
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    sequence_number INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK (
                            status IN (
                                'pending', 'in_flight', 'completed'
                            )
                        )
                )
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS batches (
                    batch_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL
                        CHECK (
                            status IN ('in_flight', 'acknowledged')
                        )
                )
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS batch_events (
                    batch_id TEXT NOT NULL,
                    sequence_number INTEGER NOT NULL UNIQUE,
                    PRIMARY KEY (batch_id, sequence_number),
                    FOREIGN KEY (batch_id)
                        REFERENCES batches(batch_id),
                    FOREIGN KEY (sequence_number)
                        REFERENCES events(sequence_number)
                )
                """
            )

    def _recover_in_flight_batches(self) -> None:
        """Return unfinished batches to the pending queue after restart."""
        with self._connection:
            self._connection.execute(
                """
                UPDATE events
                SET status = 'pending'
                WHERE status = 'in_flight'
                """
            )
            self._connection.execute(
                """
                DELETE FROM batch_events
                WHERE batch_id IN (
                    SELECT batch_id
                    FROM batches
                    WHERE status = 'in_flight'
                )
                """
            )
            self._connection.execute(
                "DELETE FROM batches WHERE status = 'in_flight'"
            )

    def publish(self, event: Any) -> int:
        """Persist a JSON-serializable event and return its sequence."""
        event_json = json.dumps(
            event,
            ensure_ascii=False,
            allow_nan=False,
        )
        created_at = float(self._clock())

        with self._connection:
            cursor = self._connection.execute(
                """
                INSERT INTO events (event_json, created_at)
                VALUES (?, ?)
                """,
                (event_json, created_at),
            )

        return int(cursor.lastrowid)

    def size(self) -> int:
        """Return the number of events waiting to be processed."""
        row = self._connection.execute(
            """
            SELECT COUNT(*) AS event_count
            FROM events
            WHERE status = 'pending'
            """
        ).fetchone()
        return int(row["event_count"])

    def oldest_event_time(self) -> float | None:
        """Return the oldest pending event's arrival time."""
        row = self._connection.execute(
            """
            SELECT created_at
            FROM events
            WHERE status = 'pending'
            ORDER BY sequence_number
            LIMIT 1
            """
        ).fetchone()

        return None if row is None else float(row["created_at"])

    def consume_batch(self, max_events: int) -> list[Any]:
        """Claim pending events in publication order."""
        if (
            not isinstance(max_events, int)
            or isinstance(max_events, bool)
            or max_events <= 0
        ):
            raise ValueError("max_events must be a positive integer")

        batch_id = str(uuid.uuid4())

        with self._connection:
            rows = self._connection.execute(
                """
                SELECT sequence_number, event_json
                FROM events
                WHERE status = 'pending'
                ORDER BY sequence_number
                LIMIT ?
                """,
                (max_events,),
            ).fetchall()

            if not rows:
                return []

            self._connection.execute(
                "INSERT INTO batches (batch_id, status) VALUES (?, ?)",
                (batch_id, "in_flight"),
            )

            for row in rows:
                self._connection.execute(
                    """
                    INSERT INTO batch_events (batch_id, sequence_number)
                    VALUES (?, ?)
                    """,
                    (batch_id, row["sequence_number"]),
                )
                self._connection.execute(
                    """
                    UPDATE events
                    SET status = 'in_flight'
                    WHERE sequence_number = ?
                    """,
                    (row["sequence_number"],),
                )

        batch = [json.loads(row["event_json"]) for row in rows]
        self._active_batches[id(batch)] = batch_id
        return batch

    def restore_batch(self, batch: list[Any]) -> None:
        """Return an uncompleted batch to the pending queue."""
        if not batch:
            return

        batch_id = self._active_batches.get(id(batch))
        if batch_id is None:
            raise ValueError(
                "Cannot restore a batch not consumed by this stream"
            )

        with self._connection:
            self._connection.execute(
                """
                UPDATE events
                SET status = 'pending'
                WHERE sequence_number IN (
                    SELECT sequence_number
                    FROM batch_events
                    WHERE batch_id = ?
                )
                """,
                (batch_id,),
            )
            self._connection.execute(
                "DELETE FROM batch_events WHERE batch_id = ?",
                (batch_id,),
            )
            self._connection.execute(
                "DELETE FROM batches WHERE batch_id = ?",
                (batch_id,),
            )

        self._active_batches.pop(id(batch), None)

    def acknowledge_batch(self, batch: list[Any]) -> None:
        """Mark a successfully processed batch as completed."""
        if not batch:
            return

        batch_id = self._active_batches.get(id(batch))
        if batch_id is None:
            raise ValueError(
                "Cannot acknowledge a batch not consumed by this stream"
            )

        with self._connection:
            self._connection.execute(
                """
                UPDATE events
                SET status = 'completed'
                WHERE sequence_number IN (
                    SELECT sequence_number
                    FROM batch_events
                    WHERE batch_id = ?
                )
                """,
                (batch_id,),
            )
            self._connection.execute(
                """
                UPDATE batches
                SET status = 'acknowledged'
                WHERE batch_id = ?
                """,
                (batch_id,),
            )

        # Keep batch_events and the acknowledged batch for audit history.
        self._active_batches.pop(id(batch), None)

    def close(self) -> None:
        """Close the SQLite connection."""
        self._connection.close()

    def __enter__(self) -> PersistentEventStream:
        return self

    def __exit__(
        self,
        exc_type: Any,
        exc_value: Any,
        traceback: Any,
    ) -> None:
        self.close()
