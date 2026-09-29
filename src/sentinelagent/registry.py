from __future__ import annotations

from typing import Dict

from .models import AgentRecord, AgentStatus, utc_now


class AgentRegistry:
    """
    In-memory authoritative registry for agent identity,
    permissions, status, and replacement relationships.

    Persistence will be added later.
    """

    def __init__(self) -> None:
        self._agents: Dict[str, AgentRecord] = {}

    def register(self, agent: AgentRecord) -> AgentRecord:
        if agent.runtime_agent_id in self._agents:
            raise ValueError(
                f"Runtime agent already registered: {agent.runtime_agent_id}"
            )

        # Only one ACTIVE runtime should normally exist for a logical role.
        active_runtime = self.get_active_runtime(agent.logical_agent_id)

        if active_runtime is not None and agent.status == AgentStatus.ACTIVE:
            raise ValueError(
                f"Logical agent already has an ACTIVE runtime: "
                f"{active_runtime.runtime_agent_id}"
            )

        self._agents[agent.runtime_agent_id] = agent
        return agent

    def get(self, runtime_agent_id: str) -> AgentRecord:
        try:
            return self._agents[runtime_agent_id]
        except KeyError as exc:
            raise KeyError(
                f"Unknown runtime agent: {runtime_agent_id}"
            ) from exc

    def all_agents(self) -> list[AgentRecord]:
        return list(self._agents.values())

    def get_active_runtime(
        self,
        logical_agent_id: str,
    ) -> AgentRecord | None:
        for agent in self._agents.values():
            if (
                agent.logical_agent_id == logical_agent_id
                and agent.status == AgentStatus.ACTIVE
            ):
                return agent

        return None

    def activate(self, runtime_agent_id: str) -> AgentRecord:
        agent = self.get(runtime_agent_id)

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

        agent.status = AgentStatus.ACTIVE

        if agent.activated_at is None:
            agent.activated_at = utc_now()

        return agent

    def mark_suspicious(self, runtime_agent_id: str) -> AgentRecord:
        agent = self.get(runtime_agent_id)

        if agent.status == AgentStatus.QUARANTINED:
            raise ValueError(
                "A quarantined runtime cannot become SUSPICIOUS."
            )

        agent.status = AgentStatus.SUSPICIOUS
        return agent

    def quarantine(
        self,
        runtime_agent_id: str,
    ) -> AgentRecord:
        agent = self.get(runtime_agent_id)

        agent.status = AgentStatus.QUARANTINED
        agent.quarantined_at = utc_now()

        return agent

    def create_replacement(
        self,
        quarantined_runtime_id: str,
        new_runtime_id: str,
        new_credentials_reference: str,
    ) -> AgentRecord:
        old_agent = self.get(quarantined_runtime_id)

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

        self.register(replacement)

        # Preserve the old rogue runtime in QUARANTINED state.
        old_agent.replaced_by = new_runtime_id

        self.activate(new_runtime_id)

        return replacement

    def is_authorized(
        self,
        source_runtime_id: str,
        target_runtime_id: str,
        requested_permission: str,
    ) -> tuple[bool, str]:
        source = self.get(source_runtime_id)
        target = self.get(target_runtime_id)

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