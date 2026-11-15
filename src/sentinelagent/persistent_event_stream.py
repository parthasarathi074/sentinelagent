
"""SQLite-backed event storage for SentinelAgent.

Events are persisted as JSON and retrieved in publication order.
Completed events remain available for audit history.
Interrupted batches are returned to the pending queue on restart.
Event state transitions and lifecycle audit records are committed atomically.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable


class PersistentEventStream:
    """Store events durably and support audited batch restoration."""

    def __init__(
        self,
        database_path: str | Path,
        clock: Callable[[], float] = time.time,
        audit_trail: Any = None,
        audit_event_factory: (
            Callable[[dict[str, Any]], dict[str, Any]] | None
        ) = None,
    ) -> None:
        self.database_path = str(database_path)
        self._clock = clock
        self._active_batches: dict[int, str] = {}
        self._audit_trail = audit_trail
        self._audit_event_factory = audit_event_factory
        self._owns_connection = audit_trail is None

        if self.database_path != ":memory:":
            Path(self.database_path).parent.mkdir(
                parents=True, exist_ok=True
            )

        if audit_trail is not None:
            audit_path = str(audit_trail.database_path)

            if (
                Path(self.database_path).resolve()
                != Path(audit_path).resolve()
            ):
                raise ValueError(
                    "event stream and audit trail must use the same database"
                )

            self._connection = audit_trail._connection
        else:
            self._connection = sqlite3.connect(self.database_path)

        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")

        try:
            self._validate_existing_schema()
            self._create_tables()
            self._recover_in_flight_batches()
        except Exception:
            if self._owns_connection:
                self._connection.close()
            raise

    @contextmanager
    def _transaction(self):
        """Run database operations in an explicit SQLite transaction."""
        lock = (
            self._audit_trail._lock
            if self._audit_trail is not None
            else None
        )

        if lock is not None:
            lock.acquire()

        try:
            self._connection.execute("BEGIN IMMEDIATE")

            try:
                yield
                self._connection.execute("COMMIT")
            except Exception:
                if self._connection.in_transaction:
                    try:
                        self._connection.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                raise
        finally:
            if lock is not None:
                lock.release()

    def _record_lifecycle_in_transaction(
        self,
        event_type: str,
        batch_id: str,
        event_count: int,
        error_type: str | None = None,
    ) -> None:
        """Append a lifecycle record inside the caller's transaction."""
        if self._audit_trail is None:
            return

        audit_event = {
            "event_type": event_type,
            "batch_id": batch_id,
            "event_count": event_count,
        }

        if error_type is not None:
            audit_event["error_type"] = error_type

        self._audit_trail.record_in_transaction(
            self._connection,
            audit_event,
        )

    def _validate_existing_schema(self) -> None:
        """Reject an incompatible existing events table without modifying it."""
        row = self._connection.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'table' AND name = 'events'
            """
        ).fetchone()

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
        with self._transaction():
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
        """Restore interrupted batches and audit recovery atomically."""
        with self._transaction():
            interrupted_batches = self._connection.execute(
                """
                SELECT
                    batches.batch_id,
                    COUNT(events.sequence_number) AS event_count
                FROM batches
                JOIN batch_events USING (batch_id)
                JOIN events USING (sequence_number)
                WHERE batches.status = 'in_flight'
                  AND events.status = 'in_flight'
                GROUP BY batches.batch_id
                ORDER BY batches.batch_id
                """
            ).fetchall()

            self._connection.execute(
                """
                UPDATE events
                SET status = 'pending'
                WHERE status = 'in_flight'
                """
            )

            for row in interrupted_batches:
                self._record_lifecycle_in_transaction(
                    event_type="BATCH_RECOVERED",
                    batch_id=str(row["batch_id"]),
                    event_count=int(row["event_count"]),
                )

    def publish(
        self,
        event: Any,
        audit_event: dict[str, Any] | None = None,
    ) -> int:
        """Persist an event and its required audit record atomically."""
        event_json = json.dumps(
            event,
            ensure_ascii=False,
            allow_nan=False,
        )
        created_at = float(self._clock())

        if audit_event is not None and self._audit_trail is None:
            raise ValueError(
                "audit_event requires a configured PersistentAuditTrail"
            )

        if audit_event is None and self._audit_event_factory is not None:
            audit_event = self._audit_event_factory(event)

        if audit_event is not None and self._audit_trail is None:
            raise ValueError(
                "audit_event requires a configured PersistentAuditTrail"
            )

        with self._transaction():
            cursor = self._connection.execute(
                """
                INSERT INTO events (event_json, created_at)
                VALUES (?, ?)
                """,
                (event_json, created_at),
            )

            if audit_event is not None:
                self._audit_trail.record_in_transaction(
                    self._connection,
                    audit_event,
                )

            sequence_number = int(cursor.lastrowid)

        return sequence_number

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
        is_retry = False
        batch_rows: list[sqlite3.Row] = []

        with self._transaction():
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

            sequence_numbers = [
                int(row["sequence_number"]) for row in rows
            ]
            placeholders = ", ".join("?" for _ in sequence_numbers)

            linked_batches = self._connection.execute(
                f"""
                SELECT DISTINCT batch_events.batch_id, batches.status
                FROM batch_events
                JOIN batches USING (batch_id)
                WHERE batch_events.sequence_number IN ({placeholders})
                """,
                sequence_numbers,
            ).fetchall()

            if any(
                row["status"] != "in_flight"
                for row in linked_batches
            ):
                raise RuntimeError(
                    "Pending events are linked to a completed batch"
                )

            linked_batch_ids = [
                str(row["batch_id"]) for row in linked_batches
            ]

            if linked_batch_ids:
                # Retry the oldest existing batch before selecting events
                # belonging to other batches.
                existing_batch_row = self._connection.execute(
                    """
                    SELECT batch_events.batch_id
                    FROM batch_events
                    JOIN batches USING (batch_id)
                    WHERE batch_events.sequence_number IN (
                        SELECT sequence_number
                        FROM events
                        WHERE status = 'pending'
                        ORDER BY sequence_number
                        LIMIT ?
                    )
                    AND batches.status = 'in_flight'
                    ORDER BY (
                        SELECT MIN(sequence_number)
                        FROM batch_events AS existing_events
                        WHERE existing_events.batch_id =
                            batch_events.batch_id
                    )
                    LIMIT 1
                    """,
                    (max_events,),
                ).fetchone()

                existing_batch_id = str(
                    existing_batch_row["batch_id"]
                )

                existing_batch = self._connection.execute(
                    """
                    SELECT sequence_number
                    FROM batch_events
                    WHERE batch_id = ?
                    ORDER BY sequence_number
                    """,
                    (existing_batch_id,),
                ).fetchall()

                existing_sequence_numbers = [
                    int(row["sequence_number"])
                    for row in existing_batch
                ]

                selected_sequence_numbers = [
                    sequence_number
                    for sequence_number in existing_sequence_numbers
                    if sequence_number in sequence_numbers
                ][:max_events]

                remaining_sequence_numbers = [
                    sequence_number
                    for sequence_number in existing_sequence_numbers
                    if sequence_number not in selected_sequence_numbers
                ]

                if remaining_sequence_numbers:
                    remaining_batch_id = str(uuid.uuid4())

                    self._connection.execute(
                        """
                        INSERT INTO batches (batch_id, status)
                        VALUES (?, ?)
                        """,
                        (remaining_batch_id, "in_flight"),
                    )

                    for sequence_number in remaining_sequence_numbers:
                        self._connection.execute(
                            """
                            UPDATE batch_events
                            SET batch_id = ?
                            WHERE batch_id = ? AND sequence_number = ?
                            """,
                            (
                                remaining_batch_id,
                                existing_batch_id,
                                sequence_number,
                            ),
                        )

                    self._record_lifecycle_in_transaction(
                        event_type="BATCH_PROCESSING_STARTED",
                        batch_id=remaining_batch_id,
                        event_count=len(remaining_sequence_numbers),
                    )

                rows = [
                    row
                    for row in rows
                    if int(row["sequence_number"])
                    in selected_sequence_numbers
                ]
                sequence_numbers = selected_sequence_numbers
                batch_id = existing_batch_id
                is_retry = True

            if batch_id not in linked_batch_ids:
                for linked_batch_id in linked_batch_ids:
                    self._connection.execute(
                        "DELETE FROM batch_events WHERE batch_id = ?",
                        (linked_batch_id,),
                    )
                    self._connection.execute(
                        """
                        DELETE FROM batches
                        WHERE batch_id = ? AND status = 'in_flight'
                        """,
                        (linked_batch_id,),
                    )

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

            for sequence_number in sequence_numbers:
                self._connection.execute(
                    """
                    UPDATE events
                    SET status = 'in_flight'
                    WHERE sequence_number = ?
                    """,
                    (sequence_number,),
                )

            self._record_lifecycle_in_transaction(
                event_type=(
                    "BATCH_PROCESSING_RETRIED"
                    if is_retry
                    else "BATCH_PROCESSING_STARTED"
                ),
                batch_id=batch_id,
                event_count=len(rows),
            )

            batch_rows = list(rows)

        batch = [
            json.loads(row["event_json"])
            for row in batch_rows
        ]
        self._active_batches[id(batch)] = batch_id
        return batch

    def batch_id_for(self, batch: list[Any]) -> str:
        """Return the stable identifier assigned to a consumed batch."""
        batch_id = self._active_batches.get(id(batch))
        if batch_id is None:
            raise ValueError(
                "Cannot identify a batch not consumed by this stream"
            )
        return batch_id

    def restore_batch(
        self,
        batch: list[Any],
        error_type: str | None = None,
    ) -> None:
        """Return an uncompleted batch to pending and audit its failure."""
        if not batch:
            return

        batch_id = self._active_batches.get(id(batch))
        if batch_id is None:
            raise ValueError(
                "Cannot restore a batch not consumed by this stream"
            )

        with self._transaction():
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

            self._record_lifecycle_in_transaction(
                event_type="BATCH_PROCESSING_FAILED",
                batch_id=batch_id,
                event_count=len(batch),
                error_type=error_type,
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

        with self._transaction():
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

            self._record_lifecycle_in_transaction(
                event_type="BATCH_PROCESSING_SUCCEEDED",
                batch_id=batch_id,
                event_count=len(batch),
            )

        # Keep batch_events and the acknowledged batch for audit history.
        self._active_batches.pop(id(batch), None)

    def close(self) -> None:
        """Close the SQLite connection only when this stream owns it."""
        if self._owns_connection:
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