from __future__ import annotations

import json
import sqlite3
from copy import deepcopy
from pathlib import Path
from typing import Dict

from .models import AgentRecord, AgentStatus, utc_now


class AgentRegistry:
    """
    Authoritative registry for agent identity, permissions, status,
    and replacement relationships.

    Records are copied at the registry boundary. Optionally, a SQLite
    database can persist registry state across process restarts.
    """

    def __init__(
        self,
        database_path: str | Path | None = None,
    ) -> None:
        self._agents: Dict[str, AgentRecord] = {}
        self._connection: sqlite3.Connection | None = None

        if database_path is not None:
            path = str(database_path)
            if path != ":memory:":
                Path(path).parent.mkdir(parents=True, exist_ok=True)

            self._connection = sqlite3.connect(path)
            self._connection.row_factory = sqlite3.Row

            try:
                self._create_table()
                self._load_agents()
            except Exception:
                self._connection.close()
                self._connection = None
                raise

    def _create_table(self) -> None:
        assert self._connection is not None
        with self._connection:
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_registry (
                    runtime_agent_id TEXT PRIMARY KEY,
                    record_json TEXT NOT NULL
                )
                """
            )

    @staticmethod
    def _serialize(agent: AgentRecord) -> str:
        return json.dumps(
            {
                "logical_agent_id": agent.logical_agent_id,
                "runtime_agent_id": agent.runtime_agent_id,
                "role": agent.role,
                "version": agent.version,
                "permissions": sorted(agent.permissions),
                "allowed_targets": sorted(agent.allowed_targets),
                "status": agent.status.value,
                "credentials_reference": agent.credentials_reference,
                "created_at": agent.created_at,
                "activated_at": agent.activated_at,
                "quarantined_at": agent.quarantined_at,
                "replacement_for": agent.replacement_for,
                "replaced_by": agent.replaced_by,
            },
            ensure_ascii=False,
            allow_nan=False,
        )

    @staticmethod
    def _deserialize(record_json: str) -> AgentRecord:
        data = json.loads(record_json)
        data["permissions"] = set(data["permissions"])
        data["allowed_targets"] = set(data["allowed_targets"])
        data["status"] = AgentStatus(data["status"])
        return AgentRecord(**data)

    def _load_agents(self) -> None:
        assert self._connection is not None
        rows = self._connection.execute(
            """
            SELECT runtime_agent_id, record_json
            FROM agent_registry
            ORDER BY runtime_agent_id
            """
        ).fetchall()

        loaded: Dict[str, AgentRecord] = {}
        for row in rows:
            agent = self._deserialize(row["record_json"])
            if agent.runtime_agent_id != row["runtime_agent_id"]:
                raise ValueError(
                    "Registry database contains a mismatched runtime ID"
                )
            if agent.runtime_agent_id in loaded:
                raise ValueError(
                    f"Duplicate runtime agent: {agent.runtime_agent_id}"
                )
            loaded[agent.runtime_agent_id] = agent

        # Reject corrupt or inconsistent persisted state rather than
        # silently dropping records or rewriting the database.
        active_logical_ids: set[str] = set()
        for agent in loaded.values():
            if agent.status == AgentStatus.ACTIVE:
                if agent.logical_agent_id in active_logical_ids:
                    raise ValueError(
                        "Registry database contains multiple ACTIVE "
                        f"runtimes for {agent.logical_agent_id}"
                    )
                active_logical_ids.add(agent.logical_agent_id)

        self._agents = loaded

    def _save_agent(self, agent: AgentRecord) -> None:
        if self._connection is None:
            return

        record_json = self._serialize(agent)
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO agent_registry (runtime_agent_id, record_json)
                VALUES (?, ?)
                ON CONFLICT(runtime_agent_id)
                DO UPDATE SET record_json = excluded.record_json
                """,
                (agent.runtime_agent_id, record_json),
            )

    def register(self, agent: AgentRecord) -> AgentRecord:
        stored_agent = deepcopy(agent)

        if stored_agent.runtime_agent_id in self._agents:
            raise ValueError(
                f"Runtime agent already registered: "
                f"{stored_agent.runtime_agent_id}"
            )

        active_runtime = self.get_active_runtime(
            stored_agent.logical_agent_id
        )

        if (
            active_runtime is not None
            and stored_agent.status == AgentStatus.ACTIVE
        ):
            raise ValueError(
                f"Logical agent already has an ACTIVE runtime: "
                f"{active_runtime.runtime_agent_id}"
            )

        self._save_agent(stored_agent)
        self._agents[stored_agent.runtime_agent_id] = stored_agent
        return deepcopy(stored_agent)

    def get(self, runtime_agent_id: str) -> AgentRecord:
        try:
            return deepcopy(self._agents[runtime_agent_id])
        except KeyError as exc:
            raise KeyError(
                f"Unknown runtime agent: {runtime_agent_id}"
            ) from exc

    def all_agents(self) -> list[AgentRecord]:
        return deepcopy(list(self._agents.values()))

    def get_active_runtime(
        self,
        logical_agent_id: str,
    ) -> AgentRecord | None:
        for agent in self._agents.values():
            if (
                agent.logical_agent_id == logical_agent_id
                and agent.status == AgentStatus.ACTIVE
            ):
                return deepcopy(agent)
        return None

    def activate(self, runtime_agent_id: str) -> AgentRecord:
        agent = self._get_internal(runtime_agent_id)

        active_runtime = self.get_active_runtime(agent.logical_agent_id)
        if (
            active_runtime is not None
            and active_runtime.runtime_agent_id != runtime_agent_id
        ):
            raise ValueError(
                f"Cannot activate {runtime_agent_id}; "
                f"{active_runtime.runtime_agent_id} is already ACTIVE."
            )

        if agent.status == AgentStatus.QUARANTINED:
            raise ValueError(
                "A quarantined runtime cannot be directly reactivated."
            )

        previous_status = agent.status
        previous_activated_at = agent.activated_at

        agent.status = AgentStatus.ACTIVE
        if agent.activated_at is None:
            agent.activated_at = utc_now()

        try:
            self._save_agent(agent)
        except Exception:
            agent.status = previous_status
            agent.activated_at = previous_activated_at
            raise

        return deepcopy(agent)

    def mark_suspicious(self, runtime_agent_id: str) -> AgentRecord:
        agent = self._get_internal(runtime_agent_id)

        if agent.status == AgentStatus.QUARANTINED:
            raise ValueError(
                "A quarantined runtime cannot become SUSPICIOUS."
            )

        previous_status = agent.status
        agent.status = AgentStatus.SUSPICIOUS
        try:
            self._save_agent(agent)
        except Exception:
            agent.status = previous_status
            raise

        return deepcopy(agent)

    def quarantine(self, runtime_agent_id: str) -> AgentRecord:
        agent = self._get_internal(runtime_agent_id)

        previous_status = agent.status
        previous_timestamp = agent.quarantined_at

        agent.status = AgentStatus.QUARANTINED
        if agent.quarantined_at is None:
            agent.quarantined_at = utc_now()

        try:
            self._save_agent(agent)
        except Exception:
            agent.status = previous_status
            agent.quarantined_at = previous_timestamp
            raise

        return deepcopy(agent)

    def create_replacement(
        self,
        quarantined_runtime_id: str,
        new_runtime_id: str,
        new_credentials_reference: str,
    ) -> AgentRecord:
        old_agent = self._get_internal(quarantined_runtime_id)

        if old_agent.status != AgentStatus.QUARANTINED:
            raise ValueError(
                "Replacement can only be created for a quarantined runtime."
            )

        if new_runtime_id in self._agents:
            raise ValueError(
                f"Replacement runtime already exists: {new_runtime_id}"
            )

        replacement = AgentRecord(
            logical_agent_id=old_agent.logical_agent_id,
            runtime_agent_id=new_runtime_id,
            role=old_agent.role,
            version=old_agent.version,
            permissions=set(old_agent.permissions),
            allowed_targets=set(old_agent.allowed_targets),
            status=AgentStatus.REGISTERED,
            credentials_reference=new_credentials_reference,
            replacement_for=old_agent.runtime_agent_id,
        )

        # Persist both sides of the replacement relationship atomically
        # when using SQLite, so a crash cannot leave only one side saved.
        replacement.status = AgentStatus.ACTIVE
        replacement.activated_at = utc_now()
        old_previous_replaced_by = old_agent.replaced_by

        old_agent.replaced_by = new_runtime_id
        try:
            if self._connection is None:
                self._agents[new_runtime_id] = deepcopy(replacement)
                self._agents[old_agent.runtime_agent_id] = deepcopy(old_agent)
            else:
                with self._connection:
                    self._connection.execute(
                        """
                        INSERT INTO agent_registry
                            (runtime_agent_id, record_json)
                        VALUES (?, ?)
                        """,
                        (
                            replacement.runtime_agent_id,
                            self._serialize(replacement),
                        ),
                    )

                    cursor = self._connection.execute(
                        """
                        UPDATE agent_registry
                        SET record_json = ?
                        WHERE runtime_agent_id = ?
                        """,
                        (
                            self._serialize(old_agent),
                            old_agent.runtime_agent_id,
                        ),
                    )

                    if cursor.rowcount != 1:
                        raise RuntimeError(
                            "Failed to persist the old runtime "
                            "replacement relationship."
                        )

                self._agents[new_runtime_id] = deepcopy(replacement)
        except Exception:
            old_agent.replaced_by = old_previous_replaced_by
            raise

        return deepcopy(replacement)

    def is_authorized(
        self,
        source_runtime_id: str,
        target_runtime_id: str,
        requested_permission: str,
    ) -> tuple[bool, str]:
        source = self._get_internal(source_runtime_id)
        target = self._get_internal(target_runtime_id)

        if source.status != AgentStatus.ACTIVE:
            return (
                False,
                f"Source agent is not ACTIVE: {source.status.value}",
            )

        if target.status != AgentStatus.ACTIVE:
            return (
                False,
                f"Target agent is not ACTIVE: {target.status.value}",
            )

        if requested_permission not in source.permissions:
            return (
                False,
                f"Permission not granted: {requested_permission}",
            )

        if target.logical_agent_id not in source.allowed_targets:
            return (
                False,
                f"Target not authorized: {target.logical_agent_id}",
            )

        return True, "authorized"

    def _get_internal(self, runtime_agent_id: str) -> AgentRecord:
        try:
            return self._agents[runtime_agent_id]
        except KeyError as exc:
            raise KeyError(
                f"Unknown runtime agent: {runtime_agent_id}"
            ) from exc

    def close(self) -> None:
        """Close the optional persistent database connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def __enter__(self) -> AgentRegistry:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()