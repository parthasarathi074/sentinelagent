from sentinelagent.models import AgentStatus
from sentinelagent.policy import PolicyAction, PolicyDecision
from sentinelagent.registry import AgentRegistry
from sentinelagent.replacement import RuntimeReplacementManager


class PolicyEnforcer:
    """Apply policy decisions to the agent registry."""

    def __init__(self, registry: AgentRegistry) -> None:
        self.registry = registry
        self.replacement_manager = RuntimeReplacementManager(registry)
        self.replacement_runtime_ids: dict[str, str] = {}

    def enforce(
        self,
        decisions: dict[str, PolicyDecision],
    ) -> dict[str, str]:
        """Apply runtime security actions and recover quarantined runtimes."""
        if not isinstance(decisions, dict):
            raise ValueError("decisions must be a dictionary")

        applied: dict[str, str] = {}
        self.replacement_runtime_ids = {}

        for runtime_agent_id, decision in decisions.items():
            if (
                not isinstance(runtime_agent_id, str)
                or not runtime_agent_id.strip()
            ):
                continue

            if not isinstance(decision, PolicyDecision):
                raise ValueError(
                    f"Invalid policy decision for runtime: {runtime_agent_id}"
                )

            if decision.action == PolicyAction.MONITOR:
                applied[runtime_agent_id] = PolicyAction.MONITOR.value
                continue

            try:
                runtime = self.registry.get(runtime_agent_id)
            except KeyError:
                # Keep the risk decision as evidence, but never try to mutate
                # a runtime that is not registered in the authoritative registry.
                applied[runtime_agent_id] = "SKIPPED_UNKNOWN_RUNTIME"
                continue

            if decision.action == PolicyAction.MARK_SUSPICIOUS:
                if runtime.status == AgentStatus.QUARANTINED:
                    # Quarantine is a stronger terminal containment state;
                    # repeated denied attempts must not downgrade it.
                    applied[runtime_agent_id] = "ALREADY_QUARANTINED"
                else:
                    self.registry.mark_suspicious(runtime_agent_id)
                    applied[runtime_agent_id] = PolicyAction.MARK_SUSPICIOUS.value
                continue

            if decision.action == PolicyAction.QUARANTINE:
                if runtime.status != AgentStatus.QUARANTINED:
                    self.registry.quarantine(runtime_agent_id)

                replacement_runtime_id = (
                    self.replacement_manager.replace(runtime_agent_id)
                )

                self.replacement_runtime_ids[runtime_agent_id] = (
                    replacement_runtime_id
                )
                applied[runtime_agent_id] = PolicyAction.QUARANTINE.value
                continue

            raise ValueError(
                f"Unsupported policy action for runtime: {runtime_agent_id}"
            )

        return applied
