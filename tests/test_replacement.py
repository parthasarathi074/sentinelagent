import unittest

from sentinelagent.models import AgentRecord, AgentStatus
from sentinelagent.registry import AgentRegistry
from sentinelagent.replacement import RuntimeReplacementManager


class TestRuntimeReplacementManager(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = AgentRegistry()
        self.manager = RuntimeReplacementManager(self.registry)

    def _register_quarantined_runtime(self) -> AgentRecord:
        runtime = AgentRecord(
            logical_agent_id="validator-01",
            runtime_agent_id="runtime-validator-001",
            role="validator",
            version="1.0",
            permissions=["read", "validate"],
            allowed_targets=["runtime-target-001"],
            status=AgentStatus.QUARANTINED,
            credentials_reference="credentials-old",
        )
        self.registry.register(runtime)
        return runtime

    def test_replaces_quarantined_runtime(self) -> None:
        old_runtime = self._register_quarantined_runtime()

        replacement_id = self.manager.replace(
            old_runtime.runtime_agent_id
        )

        replacement = self.registry.get(replacement_id)
        old_runtime = self.registry.get(
            old_runtime.runtime_agent_id
        )

        self.assertNotEqual(
            replacement.runtime_agent_id,
            old_runtime.runtime_agent_id,
        )
        self.assertEqual(
            old_runtime.status,
            AgentStatus.QUARANTINED,
        )
        self.assertEqual(
            replacement.status,
            AgentStatus.ACTIVE,
        )
        self.assertEqual(
            replacement.replacement_for,
            old_runtime.runtime_agent_id,
        )
        self.assertEqual(
            old_runtime.replaced_by,
            replacement.runtime_agent_id,
        )

    def test_replacement_gets_new_credentials(self) -> None:
        old_runtime = self._register_quarantined_runtime()

        replacement_id = self.manager.replace(
            old_runtime.runtime_agent_id
        )
        replacement = self.registry.get(replacement_id)

        self.assertNotEqual(
            replacement.credentials_reference,
            old_runtime.credentials_reference,
        )

    def test_replacement_preserves_agent_configuration(self) -> None:
        old_runtime = self._register_quarantined_runtime()

        replacement_id = self.manager.replace(
            old_runtime.runtime_agent_id
        )
        replacement = self.registry.get(replacement_id)

        self.assertEqual(
            replacement.logical_agent_id,
            old_runtime.logical_agent_id,
        )
        self.assertEqual(replacement.role, old_runtime.role)
        self.assertEqual(replacement.version, old_runtime.version)
        self.assertEqual(
            set(replacement.permissions),
            set(old_runtime.permissions),
        )
        self.assertEqual(
            set(replacement.allowed_targets),
            set(old_runtime.allowed_targets),
        )

    def test_replacement_is_idempotent(self) -> None:
        old_runtime = self._register_quarantined_runtime()

        first = self.manager.replace(old_runtime.runtime_agent_id)
        second = self.manager.replace(old_runtime.runtime_agent_id)

        self.assertEqual(first, second)

        old_runtime = self.registry.get(
            old_runtime.runtime_agent_id
        )
        replacement = self.registry.get(first)

        self.assertEqual(
            old_runtime.status,
            AgentStatus.QUARANTINED,
        )
        self.assertEqual(
            replacement.status,
            AgentStatus.ACTIVE,
        )
        self.assertEqual(
            old_runtime.replaced_by,
            replacement.runtime_agent_id,
        )


if __name__ == "__main__":
    unittest.main()
