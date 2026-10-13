from __future__ import annotations

import json
import logging
import sqlite3
from copy import copy
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from .models import AgentStatus, utc_now
from .registry import AgentRegistry


logger = logging.getLogger(__name__)


class RecoveryState(str, Enum):
    NOT_REQUIRED = "NOT_REQUIRED"
    PENDING = "PENDING"
    RECOVERING = "RECOVERING"
    RECOVERED = "RECOVERED"
    FAILED = "FAILED"


@dataclass
class RecoveryRecord:
    logical_agent_id: str
    old_runtime_id: str
    replacement_runtime_id: str
    state: RecoveryState
    started_at: str | None = None
    completed_at: str | None = None
    reason: str | None = None
    attempt: int = 0

    def to_dict(self) -> dict[str, object]:
        return {
            "logical_agent_id": self.logical_agent_id,
            "old_runtime_id": self.old_runtime_id,
            "replacement_runtime_id": self.replacement_runtime_id,
            "state": self.state.value,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "reason": self.reason,
            "attempt": self.attempt,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> RecoveryRecord:
        attempt = data.get("attempt", 0)
        if (
            not isinstance(attempt, int)
            or isinstance(attempt, bool)
            or attempt < 0
        ):
            raise ValueError(
                "Recovery attempt must be a non-negative integer."
            )

        return cls(
            logical_agent_id=str(data["logical_agent_id"]),
            old_runtime_id=str(data["old_runtime_id"]),
            replacement_runtime_id=str(data["replacement_runtime_id"]),
            state=RecoveryState(str(data["state"])),
            started_at=data.get("started_at"),  # type: ignore[arg-type]
            completed_at=data.get("completed_at"),  # type: ignore[arg-type]
            reason=data.get("reason"),  # type: ignore[arg-type]
            attempt=attempt,
        )


class PipelineRecoveryManager:
    """
    Verify and persist pipeline recovery after runtime replacement.

    External scheduler, container, and Kafka reconnection are outside
    this manager until those integrations exist.
    """

    def __init__(
        self,
        registry: AgentRegistry,
        database_path: str | Path | None = None,
    ) -> None:
        self.registry = registry
        self._records: dict[str, RecoveryRecord] = {}
        self._connection: sqlite3.Connection | None = None

        if database_path is not None:
            self._connection = sqlite3.connect(str(database_path))
            try:
                self._connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS recovery_records (
                        old_runtime_id TEXT PRIMARY KEY,
                        record_json TEXT NOT NULL
                    )
                    """
                )
                self._connection.commit()
                self._load_records()
            except Exception:
                self._connection.close()
                self._connection = None
                raise

    def _load_records(self) -> None:
        if self._connection is None:
            return

        rows = self._connection.execute(
            "SELECT old_runtime_id, record_json FROM recovery_records"
        ).fetchall()

        loaded: dict[str, RecoveryRecord] = {}

        for old_runtime_id, record_json in rows:
            data = json.loads(record_json)
            if not isinstance(data, dict):
                raise ValueError(
                    "Stored recovery record must be an object."
                )

            record = RecoveryRecord.from_dict(data)
            if record.old_runtime_id != old_runtime_id:
                raise ValueError(
                    "Stored recovery record ID is inconsistent."
                )
            if old_runtime_id in loaded:
                raise ValueError(
                    f"Duplicate recovery record: {old_runtime_id}"
                )

            loaded[old_runtime_id] = record

        self._records = loaded

    def _save_record(self, record: RecoveryRecord) -> None:
        if self._connection is None:
            return

        payload = json.dumps(record.to_dict(), sort_keys=True)

        with self._connection:
            self._connection.execute(
                """
                INSERT INTO recovery_records (old_runtime_id, record_json)
                VALUES (?, ?)
                ON CONFLICT(old_runtime_id)
                DO UPDATE SET record_json = excluded.record_json
                """,
                (record.old_runtime_id, payload),
            )

    def _publish_record(self, record: RecoveryRecord) -> RecoveryRecord:
        """
        Persist before publishing a candidate state.

        When a record already exists, update that same object only after
        persistence succeeds. This preserves object identity for callers
        holding an earlier reference to the record.
        """
        self._save_record(record)

        existing = self._records.get(record.old_runtime_id)
        if existing is None:
            self._records[record.old_runtime_id] = record
            return record

        if existing is record:
            return existing

        existing.logical_agent_id = record.logical_agent_id
        existing.old_runtime_id = record.old_runtime_id
        existing.replacement_runtime_id = record.replacement_runtime_id
        existing.state = record.state
        existing.started_at = record.started_at
        existing.completed_at = record.completed_at
        existing.reason = record.reason
        existing.attempt = record.attempt

        return existing

    def close(self) -> None:
        """Close the optional persistent database connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def __enter__(self) -> PipelineRecoveryManager:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def recover(
        self,
        old_runtime_id: str,
        replacement_runtime_id: str,
        reason: str | None = None,
    ) -> RecoveryRecord:
        if not isinstance(old_runtime_id, str) or not old_runtime_id.strip():
            raise ValueError("old_runtime_id must be a non-empty string")

        if (
            not isinstance(replacement_runtime_id, str)
            or not replacement_runtime_id.strip()
        ):
            raise ValueError(
                "replacement_runtime_id must be a non-empty string"
            )

        existing = self._records.get(old_runtime_id)

        if existing is not None:
            if existing.replacement_runtime_id != replacement_runtime_id:
                raise ValueError(
                    "Recovery already exists for the old runtime with a "
                    "different replacement runtime."
                )

            if existing.state == RecoveryState.RECOVERED:
                logger.info(
                    "recovery_skipped logical_agent_id=%s "
                    "old_runtime_id=%s replacement_runtime_id=%s "
                    "state=%s reason=already_recovered",
                    existing.logical_agent_id,
                    old_runtime_id,
                    replacement_runtime_id,
                    existing.state.value,
                )
                return existing

            # Do not mutate a published record until its candidate state
            # has been persisted successfully.
            record = copy(existing)
        else:
            old_runtime = self.registry.get(old_runtime_id)
            record = RecoveryRecord(
                logical_agent_id=old_runtime.logical_agent_id,
                old_runtime_id=old_runtime_id,
                replacement_runtime_id=replacement_runtime_id,
                state=RecoveryState.PENDING,
            )

        record.state = RecoveryState.RECOVERING
        record.started_at = record.started_at or utc_now()
        record.completed_at = None
        record.reason = reason
        record.attempt += 1

        # A failed write leaves the previously published object unchanged.
        record = self._publish_record(record)

        logger.info(
            "recovery_started logical_agent_id=%s old_runtime_id=%s "
            "replacement_runtime_id=%s attempt=%d",
            record.logical_agent_id,
            old_runtime_id,
            replacement_runtime_id,
            record.attempt,
        )

        try:
            self._verify_recovery(
                old_runtime_id=old_runtime_id,
                replacement_runtime_id=replacement_runtime_id,
            )
        except (KeyError, ValueError) as exc:
            failed_record = copy(record)
            failed_record.state = RecoveryState.FAILED
            failed_record.completed_at = utc_now()
            failed_record.reason = str(exc)

            # Only publish FAILED if that state was persisted successfully.
            failed_record = self._publish_record(failed_record)

            logger.warning(
                "recovery_finished logical_agent_id=%s old_runtime_id=%s "
                "replacement_runtime_id=%s attempt=%d state=%s reason=%s",
                failed_record.logical_agent_id,
                old_runtime_id,
                replacement_runtime_id,
                failed_record.attempt,
                failed_record.state.value,
                failed_record.reason,
            )
            return failed_record

        recovered_record = copy(record)
        recovered_record.state = RecoveryState.RECOVERED
        recovered_record.completed_at = utc_now()

        # If this write fails, the published state remains RECOVERING.
        recovered_record = self._publish_record(recovered_record)

        logger.info(
            "recovery_finished logical_agent_id=%s old_runtime_id=%s "
            "replacement_runtime_id=%s attempt=%d state=%s",
            recovered_record.logical_agent_id,
            old_runtime_id,
            replacement_runtime_id,
            recovered_record.attempt,
            recovered_record.state.value,
        )
        return recovered_record

    def get(self, old_runtime_id: str) -> RecoveryRecord | None:
        return self._records.get(old_runtime_id)

    def snapshot(self) -> dict[str, RecoveryRecord]:
        return dict(self._records)

    def snapshot_json(self) -> dict[str, dict[str, object]]:
        return {
            runtime_id: record.to_dict()
            for runtime_id, record in self._records.items()
        }

    def _verify_recovery(
        self,
        old_runtime_id: str,
        replacement_runtime_id: str,
    ) -> None:
        old_runtime = self.registry.get(old_runtime_id)
        replacement_runtime = self.registry.get(replacement_runtime_id)

        if old_runtime.status != AgentStatus.QUARANTINED:
            raise ValueError(
                "Recovery requires the old runtime to remain QUARANTINED."
            )

        if replacement_runtime.status != AgentStatus.ACTIVE:
            raise ValueError(
                "Recovery requires the replacement runtime to be ACTIVE."
            )

        if old_runtime.logical_agent_id != replacement_runtime.logical_agent_id:
            raise ValueError(
                "Old and replacement runtimes belong to different "
                "logical agents."
            )

        if old_runtime.replaced_by != replacement_runtime.runtime_agent_id:
            raise ValueError(
                "Old runtime replacement relationship is inconsistent."
            )

        if replacement_runtime.replacement_for != old_runtime.runtime_agent_id:
            raise ValueError(
                "Replacement runtime relationship is inconsistent."
            )

        active_runtime = self.registry.get_active_runtime(
            old_runtime.logical_agent_id
        )

        if active_runtime is None:
            raise ValueError(
                "Recovery requires exactly one ACTIVE runtime, but none "
                "is ACTIVE."
            )

        if active_runtime.runtime_agent_id != replacement_runtime_id:
            raise ValueError(
                "Replacement runtime is not the authoritative ACTIVE "
                "runtime for the logical agent."
            )

        active_runtimes = [
            agent
            for agent in self.registry.all_agents()
            if agent.logical_agent_id == old_runtime.logical_agent_id
            and agent.status == AgentStatus.ACTIVE
        ]

        if len(active_runtimes) != 1:
            raise ValueError(
                "Recovery requires exactly one ACTIVE runtime for the "
                "logical agent."
            )

        if (
            replacement_runtime.credentials_reference
            == old_runtime.credentials_reference
        ):
            raise ValueError(
                "Replacement runtime must use different credentials."
            )
