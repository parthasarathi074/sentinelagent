from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .models import AgentStatus, utc_now
from .registry import AgentRegistry


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


class PipelineRecoveryManager:
    """
    Manage logical pipeline recovery after runtime replacement.

    The current implementation verifies and records recovery inside the
    SentinelAgent registry. External scheduler/container/Kafka reconnection
    is intentionally outside this manager until those integrations exist.
    """

    def __init__(self, registry: AgentRegistry) -> None:
        self.registry = registry
        self._records: dict[str, RecoveryRecord] = {}

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
                return existing

        old_runtime = self.registry.get(old_runtime_id)

        record = existing or RecoveryRecord(
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

        self._records[old_runtime_id] = record

        try:
            self._verify_recovery(
                old_runtime_id=old_runtime_id,
                replacement_runtime_id=replacement_runtime_id,
            )
        except (KeyError, ValueError) as exc:
            record.state = RecoveryState.FAILED
            record.completed_at = utc_now()
            record.reason = str(exc)
            return record

        record.state = RecoveryState.RECOVERED
        record.completed_at = utc_now()

        return record

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

        if (
            old_runtime.logical_agent_id
            != replacement_runtime.logical_agent_id
        ):
            raise ValueError(
                "Old and replacement runtimes belong to different "
                "logical agents."
            )

        if old_runtime.replaced_by != replacement_runtime.runtime_agent_id:
            raise ValueError(
                "Old runtime replacement relationship is inconsistent."
            )

        if (
            replacement_runtime.replacement_for
            != old_runtime.runtime_agent_id
        ):
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
            if (
                agent.logical_agent_id == old_runtime.logical_agent_id
                and agent.status == AgentStatus.ACTIVE
            )
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
