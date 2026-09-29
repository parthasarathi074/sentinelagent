import unittest

from sentinelagent.edge import SentinelEdge
from sentinelagent.models import (
    AgentRecord,
    AgentStatus,
    EdgeDecision,
    EdgeRequest,
)
from sentinelagent.registry import AgentRegistry


class TestSentinelEdge(unittest.TestCase):

    def setUp(self) -> None:
        self.events = []

        registry = AgentRegistry()

        agent_a = AgentRecord(
            logical_agent_id="agent-A",
            runtime_agent_id="runtime-A-001",
            role="ingest",
            version="1.0",
            permissions={"message:send"},
            allowed_targets={"agent-B"},
            credentials_reference="cred-A-001",
        )

        agent_b = AgentRecord(
            logical_agent_id="agent-B",
            runtime_agent_id="runtime-B-001",
            role="validator",
            version="1.0",
            permissions={"validation:execute"},
            allowed_targets={"agent-A"},
            credentials_reference="cred-B-001",
        )

        registry.register(agent_a)
        registry.register(agent_b)

        registry.activate("runtime-A-001")
        registry.activate("runtime-B-001")

        self.registry = registry

        self.edge = SentinelEdge(
            registry=registry,
            event_sink=self.events.append,
        )

    def make_request(
        self,
        permission: str = "message:send",
    ) -> EdgeRequest:
        return EdgeRequest(
            request_id="req-001",
            trace_id="trace-001",
            edge_id="edge-agent-A-agent-B",
            source_runtime_id="runtime-A-001",
            target_runtime_id="runtime-B-001",
            action="SEND_MESSAGE",
            requested_permission=permission,
            resource_id="record-001",
        )

    def test_authorized_request_is_allowed(self) -> None:
        result = self.edge.handle(self.make_request())

        self.assertEqual(result.decision, EdgeDecision.ALLOW)
        self.assertEqual(len(self.events), 1)
        self.assertEqual(
            self.events[0]["decision"],
            "ALLOW",
        )

    def test_unauthorized_permission_is_denied(self) -> None:
        result = self.edge.handle(
            self.make_request("data:write")
        )

        self.assertEqual(result.decision, EdgeDecision.DENY)
        self.assertIn(
            "Permission not granted",
            result.reason,
        )

    def test_quarantined_target_is_denied(self) -> None:
        self.registry.quarantine("runtime-B-001")

        result = self.edge.handle(self.make_request())

        self.assertEqual(result.decision, EdgeDecision.DENY)
        self.assertIn(
            "QUARANTINED",
            result.reason,
        )

    def test_event_contains_security_context(self) -> None:
        result = self.edge.handle(self.make_request())

        event = result.event

        self.assertEqual(
            event["edge_id"],
            "edge-agent-A-agent-B",
        )

        self.assertEqual(
            event["source_agent"]["logical_id"],
            "agent-A",
        )

        self.assertEqual(
            event["target_agent"]["logical_id"],
            "agent-B",
        )

        self.assertEqual(
            event["decision"],
            "ALLOW",
        )


if __name__ == "__main__":
    unittest.main()