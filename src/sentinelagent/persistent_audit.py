"""SQLite-backed, hash-chained audit trail for SentinelAgent security decisions.

Only explicitly selected request metadata and the authorization decision are
stored. Request bodies, credentials, and arbitrary payload fields are excluded.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

from sentinelagent.models import utc_now

_GENESIS_HASH = "0" * 64


def _canonical_json(value: dict[str, Any]) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _entry_hash(previous_hash: str, event_json: str) -> str:
    return hashlib.sha256((previous_hash + "\n" + event_json).encode("utf-8")).hexdigest()


class PersistentAuditTrail:
    """Append-oriented durable security audit storage with chain verification.

    The database is stored separately from the agent-registry database. The
    public API intentionally has no update/delete operation for prior records.
    Hash chaining detects changes/removals when a later verification is run; it
    is tamper-evident, not a substitute for access controls or off-host backups.
    """

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)
        if str(database_path) != ":memory:":
            self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(
            str(database_path),
            timeout=5.0,
            isolation_level=None,
            check_same_thread=False,
        )
        with self._lock:
            self._connection.execute("PRAGMA busy_timeout = 5000")
            self._connection.execute("PRAGMA synchronous = FULL")
            self._connection.execute("PRAGMA journal_mode = WAL")
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS audit_entries (
                    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    captured_at TEXT NOT NULL,
                    event_json TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    event_hash TEXT NOT NULL UNIQUE
                )
                """
            )
            if not self.verify_integrity():
                self._connection.close()
                raise ValueError("Audit trail integrity check failed")

    def record(self, event: dict[str, Any]) -> dict[str, Any]:
        """Append one event and return its durable audit ID and chain hash."""
        if not isinstance(event, dict):
            raise TypeError("event must be a dictionary")
        safe_event = dict(event)
        safe_event.setdefault("captured_at", utc_now())
        if not isinstance(safe_event.get("captured_at"), str):
            raise ValueError("event captured_at must be a string")
        if not isinstance(safe_event.get("event_type"), str) or not safe_event["event_type"].strip():
            raise ValueError("event_type must be a non-empty string")
        event_json = _canonical_json(safe_event)

        with self._lock:
            connection = self._connection
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT event_hash FROM audit_entries ORDER BY audit_id DESC LIMIT 1"
                ).fetchone()
                previous_hash = row[0] if row else _GENESIS_HASH
                event_hash = _entry_hash(previous_hash, event_json)
                cursor = connection.execute(
                    """
                    INSERT INTO audit_entries
                        (captured_at, event_json, previous_hash, event_hash)
                    VALUES (?, ?, ?, ?)
                    """,
                    (safe_event["captured_at"], event_json, previous_hash, event_hash),
                )
                audit_id = int(cursor.lastrowid)
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

        return {
            **safe_event,
            "audit_id": audit_id,
            "previous_hash": previous_hash,
            "event_hash": event_hash,
        }

    def list_recent(self, limit: int = 100) -> list[dict[str, Any]]:
        """Return up to ``limit`` newest records, newest first."""
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValueError("limit must be an integer between 1 and 1000")
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT audit_id, captured_at, event_json, previous_hash, event_hash
                FROM audit_entries ORDER BY audit_id DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
        records = []
        for audit_id, captured_at, event_json, previous_hash, event_hash in rows:
            record = json.loads(event_json)
            record.update({
                "audit_id": int(audit_id),
                "captured_at": captured_at,
                "previous_hash": previous_hash,
                "event_hash": event_hash,
            })
            records.append(record)
        return records

    def verify_integrity(self) -> bool:
        """Verify row order, predecessor links, and every event hash."""
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT captured_at, event_json, previous_hash, event_hash
                FROM audit_entries ORDER BY audit_id ASC
                """
            ).fetchall()
        previous_hash = _GENESIS_HASH
        for captured_at, event_json, stored_previous, stored_hash in rows:
            if stored_previous != previous_hash:
                return False
            if _entry_hash(previous_hash, event_json) != stored_hash:
                return False
            try:
                event = json.loads(event_json)
            except (TypeError, json.JSONDecodeError):
                return False
            if not isinstance(event, dict) or event.get("captured_at") != captured_at:
                return False
            previous_hash = stored_hash
        return True

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __enter__(self) -> "PersistentAuditTrail":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()
