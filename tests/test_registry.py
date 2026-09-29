import unittest

from sentinelagent.models import AgentRecord, AgentStatus
from sentinelagent.registry import AgentRegistry


class TestAgentRegistry(unittest.TestCase):

    def setUp(self) -> None:
        self.registry = AgentRegistry()

        self.agent_a = AgentRecord(
            logical_agent_id="agent-A",
            runtime_agent_id="runtime-A-001",
            role="ingest",
            version="1.0",
            permissions={"message:send"},
            allowed_targets={"agent-B"},
            credentials_reference="cred-A-001",
        )

        self.agent_b = AgentRecord(
            logical_agent_id="agent-B",
            runtime_agent_id="runtime-B-001",
            role="validator",
            version="1.0",
            permissions={"validation:execute"},
            allowed_targets={"agent-A"},
            credentials_reference="cred-B-001",
        )

        self.registry.register(self.agent_a)
        self.registry.register(self.agent_b)

        self.registry.activate(self.agent_a.runtime_agent_id)
        self.registry.activate(self.agent_b.runtime_agent_id)

    def test_agent_is_registered_and_active(self) -> None:
        agent = self.registry.get("runtime-A-001")

        self.assertEqual(agent.logical_agent_id, "agent-A")
        self.assertEqual(agent.status, AgentStatus.ACTIVE)

    def test_authorized_communication(self) -> None:
        allowed, reason = self.registry.is_authorized(
            source_runtime_id="runtime-A-001",
            target_runtime_id="runtime-B-001",
            requested_permission="message:send",
        )

        self.assertTrue(allowed)
        self.assertEqual(reason, "authorized")

    def test_unauthorized_permission_is_denied(self) -> None:
        allowed, reason = self.registry.is_authorized(
            source_runtime_id="runtime-A-001",
            target_runtime_id="runtime-B-001",
            requested_permission="data:write",
        )

        self.assertFalse(allowed)
        self.assertIn("Permission not granted", reason)

    def test_quarantine_blocks_agent(self) -> None:
        self.registry.quarantine("runtime-B-001")

        allowed, reason = self.registry.is_authorized(
            source_runtime_id="runtime-A-001",
            target_runtime_id="runtime-B-001",
            requested_permission="message:send",
        )

        self.assertFalse(allowed)
        self.assertIn("QUARANTINED", reason)

    def test_replacement_keeps_old_runtime_quarantined(self) -> None:
        self.registry.quarantine("runtime-B-001")

        replacement = self.registry.create_replacement(
            quarantined_runtime_id="runtime-B-001",
            new_runtime_id="runtime-B-002",
            new_credentials_reference="cred-B-002",
        )

        old_agent = self.registry.get("runtime-B-001")
        new_agent = self.registry.get("runtime-B-002")

        self.assertEqual(
            old_agent.status,
            AgentStatus.QUARANTINED,
        )

        self.assertEqual(
            new_agent.status,
            AgentStatus.ACTIVE,
        )

        self.assertEqual(
            new_agent.replacement_for,
            "runtime-B-001",
        )

        self.assertEqual(
            old_agent.replaced_by,
            "runtime-B-002",
        )

        self.assertNotEqual(
            old_agent.runtime_agent_id,
            new_agent.runtime_agent_id,
        )

        self.assertNotEqual(
            old_agent.credentials_reference,
            new_agent.credentials_reference,
        )


if __name__ == "__main__":
    unittest.main()