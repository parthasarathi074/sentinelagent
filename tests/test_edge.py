import logging
import unittest

from sentinelagent.edge import SentinelEdge
from sentinelagent.event_stream import EventStream
from sentinelagent.models import (
    AgentRecord,
    EdgeDecision,
    EdgeRequest,
)
from sentinelagent.registry import AgentRegistry


class TestSentinelEdge(unittest.TestCase):
    def setUp(self) -> None:
        self.events = []

        self.registry = AgentRegistry()

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

        self.registry.register(agent_a)
        self.registry.register(agent_b)

        self.registry.activate("runtime-A-001")
        self.registry.activate("runtime-B-001")

        self.edge = SentinelEdge(
            registry=self.registry,
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
        self.assertEqual(self.events[0]["decision"], "ALLOW")

    def test_decision_emits_structured_log_with_runtime_context(self) -> None:
        with self.assertLogs("sentinelagent.edge", level=logging.INFO) as captured:
            result = self.edge.handle(self.make_request())

        self.assertEqual(result.decision, EdgeDecision.ALLOW)
        message = "\n".join(captured.output)
        self.assertIn("sentinel_edge_decision", message)
        self.assertIn("req-001", message)
        self.assertIn("runtime-A-001", message)
        self.assertIn("runtime-B-001", message)
        self.assertIn("decision='ALLOW'", message)

    def test_event_is_published_to_event_stream(self) -> None:
        stream = EventStream()

        edge = SentinelEdge(
            registry=self.registry,
            event_sink=stream.publish,
        )

        request = self.make_request()
        result = edge.handle(request)

        self.assertEqual(result.decision, EdgeDecision.ALLOW)
        self.assertEqual(stream.size(), 1)

        events = stream.consume_batch(1)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_id"], request.request_id)

    def test_unauthorized_permission_is_denied(self) -> None:
        result = self.edge.handle(self.make_request("data:write"))

        self.assertEqual(result.decision, EdgeDecision.DENY)
        self.assertIn("Permission not granted", result.reason)

    def test_quarantined_target_is_denied(self) -> None:
        self.registry.quarantine("runtime-B-001")

        result = self.edge.handle(self.make_request())

        self.assertEqual(result.decision, EdgeDecision.DENY)
        self.assertIn("QUARANTINED", result.reason)

    def test_event_contains_security_context(self) -> None:
        result = self.edge.handle(self.make_request())

        event = result.event

        self.assertEqual(event["edge_id"], "edge-agent-A-agent-B")
        self.assertEqual(
            event["source_agent"]["logical_id"],
            "agent-A",
        )
        self.assertEqual(
            event["target_agent"]["logical_id"],
            "agent-B",
        )
        self.assertEqual(event["decision"], "ALLOW")

    def test_blank_required_field_is_denied_and_audited(self) -> None:
        request = self.make_request()
        request.requested_permission = " "

        result = self.edge.handle(request)

        self.assertEqual(result.decision, EdgeDecision.DENY)
        self.assertIn("requested_permission", result.reason)
        self.assertEqual(len(self.events), 1)
        self.assertEqual(self.events[0]["decision"], "DENY")

    def test_non_string_required_field_is_denied_and_audited(self) -> None:
        request = self.make_request()
        request.source_runtime_id = None

        result = self.edge.handle(request)

        self.assertEqual(result.decision, EdgeDecision.DENY)
        self.assertIn("source_runtime_id", result.reason)
        self.assertEqual(self.events[0]["decision"], "DENY")

    def test_invalid_optional_field_is_denied_and_audited(self) -> None:
        request = self.make_request()
        request.payload_hash = 123

        result = self.edge.handle(request)

        self.assertEqual(result.decision, EdgeDecision.DENY)
        self.assertIn("payload_hash", result.reason)
        self.assertEqual(self.events[0]["decision"], "DENY")

    def test_non_edge_request_is_denied_without_crashing(self) -> None:
        result = self.edge.handle(None)

        self.assertEqual(result.decision, EdgeDecision.DENY)
        self.assertIn("EdgeRequest", result.reason)
        self.assertEqual(len(self.events), 1)
        self.assertEqual(self.events[0]["decision"], "DENY")

    def test_unknown_runtime_is_denied_and_audited(self) -> None:
        request = self.make_request()
        request.source_runtime_id = "unknown-runtime"

        result = self.edge.handle(request)

        self.assertEqual(result.decision, EdgeDecision.DENY)
        self.assertIn("Unknown runtime agent", result.reason)
        self.assertEqual(self.events[0]["decision"], "DENY")
        self.assertEqual(
            self.events[0]["source_agent"]["runtime_id"],
            "unknown-runtime",
        )
        self.assertEqual(
            self.events[0]["target_agent"]["runtime_id"],
            "runtime-B-001",
        )

    def test_quarantined_runtime_is_blocked_and_replacement_can_reconnect(
        self,
    ) -> None:
        from sentinelagent.models import AgentStatus
        from sentinelagent.replacement import RuntimeReplacementManager

        old_runtime_id = "runtime-A-001"
        self.registry.quarantine(old_runtime_id)

        replacement_manager = RuntimeReplacementManager(self.registry)
        replacement_runtime_id = replacement_manager.replace(old_runtime_id)

        old_result = self.edge.handle(self.make_request())

        replacement_request = self.make_request()
        replacement_request.request_id = "req-replacement-001"
        replacement_request.trace_id = "trace-replacement-001"
        replacement_request.source_runtime_id = replacement_runtime_id
        replacement_result = self.edge.handle(replacement_request)

        self.assertEqual(old_result.decision, EdgeDecision.DENY)
        self.assertIn(
            "Source agent is not ACTIVE: QUARANTINED",
            old_result.reason,
        )
        self.assertEqual(replacement_result.decision, EdgeDecision.ALLOW)
        self.assertEqual(
            self.registry.get(old_runtime_id).status,
            AgentStatus.QUARANTINED,
        )
        replacement = self.registry.get(replacement_runtime_id)
        self.assertEqual(replacement.status, AgentStatus.ACTIVE)
        self.assertEqual(replacement.replacement_for, old_runtime_id)
        self.assertEqual(
            [event["decision"] for event in self.events],
            ["DENY", "ALLOW"],
        )
        self.assertEqual(
            self.events[1]["source_agent"]["runtime_id"],
            replacement_runtime_id,
        )

    def test_audit_sink_failure_is_not_silenced(self) -> None:
        def failing_sink(event):
            raise RuntimeError("audit storage unavailable")

        edge = SentinelEdge(
            registry=self.registry,
            event_sink=failing_sink,
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "audit storage unavailable",
        ):
            edge.handle(self.make_request())


if __name__ == "__main__":
    unittest.main()
