from typing import Any

from sentinelagent.policy import PolicyAction, PolicyDecision
from sentinelagent.registry import AgentRegistry


class PolicyEnforcer:
    """Apply policy decisions to the agent registry."""

    def __init__(self, registry: AgentRegistry) -> None:
        self.registry = registry

    def enforce(
        self,
        decisions: dict[str, PolicyDecision],
    ) -> dict[str, str]:
        """Apply runtime security actions and return applied actions."""
        if not isinstance(decisions, dict):
            raise ValueError("decisions must be a dictionary")

        applied: dict[str, str] = {}

        for runtime_agent_id, decision in decisions.items():
            if not isinstance(runtime_agent_id, str) or not runtime_agent_id.strip():
                continue

            if not isinstance(decision, PolicyDecision):
                raise ValueError(
                    f"Invalid policy decision for runtime: {runtime_agent_id}"
                )

            if decision.action == PolicyAction.MONITOR:
                applied[runtime_agent_id] = PolicyAction.MONITOR.value
                continue

            if decision.action == PolicyAction.MARK_SUSPICIOUS:
                self.registry.mark_suspicious(runtime_agent_id)
                applied[runtime_agent_id] = PolicyAction.MARK_SUSPICIOUS.value
                continue

            if decision.action == PolicyAction.QUARANTINE:
                self.registry.quarantine(runtime_agent_id)
                applied[runtime_agent_id] = PolicyAction.QUARANTINE.value
                continue

            raise ValueError(
                f"Unsupported policy action for runtime: {runtime_agent_id}"
            )

        return applied
