from uuid import uuid4

from sentinelagent.models import AgentStatus
from sentinelagent.registry import AgentRegistry


class RuntimeReplacementManager:
    """Create clean replacement runtimes for quarantined agents."""

    def __init__(self, registry: AgentRegistry) -> None:
        self.registry = registry
        self._replacement_ids: dict[str, str] = {}

    def replace(self, quarantined_runtime_id: str) -> str:
        """Create and activate a replacement for a quarantined runtime."""
        if (
            not isinstance(quarantined_runtime_id, str)
            or not quarantined_runtime_id.strip()
        ):
            raise ValueError(
                "quarantined_runtime_id must be a non-empty string"
            )

        existing_replacement = self._replacement_ids.get(
            quarantined_runtime_id
        )
        if existing_replacement is not None:
            return existing_replacement

        runtime = self.registry.get(quarantined_runtime_id)

        if runtime.status != AgentStatus.QUARANTINED:
            raise ValueError(
                f"Runtime '{quarantined_runtime_id}' must be quarantined "
                "before replacement"
            )

        if runtime.replaced_by is not None:
            self._replacement_ids[quarantined_runtime_id] = runtime.replaced_by
            return runtime.replaced_by

        new_runtime_id = (
            f"runtime-{runtime.logical_agent_id}-"
            f"{uuid4().hex[:12]}"
        )
        new_credentials_reference = (
            f"credentials-{uuid4().hex}"
        )

        replacement = self.registry.create_replacement(
            quarantined_runtime_id=quarantined_runtime_id,
            new_runtime_id=new_runtime_id,
            new_credentials_reference=new_credentials_reference,
        )

        self._replacement_ids[quarantined_runtime_id] = (
            replacement.runtime_agent_id
        )

        return replacement.runtime_agent_id
