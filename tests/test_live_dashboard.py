from __future__ import annotations

import json
import unittest

from sentinelagent.analysis_pipeline import AnalysisPipeline
from sentinelagent.live_dashboard import LiveDashboardSnapshotProvider
from sentinelagent.models import AgentRecord, AgentStatus
from sentinelagent.registry import AgentRegistry


class FakePipeline:
    def __init__(self, registry, snapshot=None, metrics=None, event_stream=None):
        self.registry = registry
        self._snapshot = snapshot or {}
        self._metrics = metrics or {"processed": 0, "failed": 0, "retried": 0, "quarantined": 0}
        self.event_stream = event_stream

    def snapshot(self):
        return self._snapshot

    def metrics_snapshot(self):
        return self._metrics


class FakeEventStream:
    def __init__(self, pending):
        self.pending = pending

    def size(self):
        return self.pending


class LiveDashboardTests(unittest.TestCase):
    def setUp(self):
        self.registry = AgentRegistry()
        self.registry.register(AgentRecord(
            logical_agent_id="agent-A",
            runtime_agent_id="runtime-A-1",
            role="worker",
            version="1.0",
            permissions={"message:send"},
            allowed_targets={"agent-B"},
            status=AgentStatus.ACTIVE,
            credentials_reference="secret-ref-must-not-leak",
        ))

    def test_live_agent_state_metrics_and_queue_are_exposed(self):
        pipeline = FakePipeline(
            self.registry,
            snapshot={"runtime_risk_assessments": {}, "policy_decisions_json": {}},
            metrics={"processed": 7, "failed": 1, "retried": 2, "quarantined": 0},
            event_stream=FakeEventStream(3),
        )
        snapshot = LiveDashboardSnapshotProvider(pipeline)()
        self.assertEqual(snapshot["metrics"]["processed"], 7)
        self.assertEqual(snapshot["agent_status"][0]["status"], "ACTIVE")
        self.assertEqual(snapshot["event_queue"], {"available": True, "pending_events": 3})
        encoded = json.dumps(snapshot)
        self.assertNotIn("secret-ref-must-not-leak", encoded)

    def test_current_risk_and_policy_decisions_appear(self):
        pipeline = FakePipeline(self.registry, snapshot={
            "batches_processed": 4,
            "runtime_risk_assessments": {
                "runtime-A-1": {"risk_score": 55, "risk_level": "HIGH", "reasons": ["SHARED_TARGET_INTERACTIONS"]}
            },
            "policy_decisions_json": {
                "runtime-A-1": {"risk_score": 55, "risk_level": "HIGH", "action": "QUARANTINE", "reasons": ["SHARED_TARGET_INTERACTIONS"]}
            },
            "collusion_assessment": {"detected": True},
            "recovery_results": {},
            "runtime_target_interactions": {"runtime-A-1": {"runtime-B-1": 4}},
        })
        snapshot = LiveDashboardSnapshotProvider(pipeline)()
        self.assertEqual(snapshot["batches_processed"], 4)
        self.assertEqual(snapshot["suspicious_activity"][0]["risk_level"], "HIGH")
        self.assertEqual(snapshot["security_events"][0]["event_type"], "CURRENT_POLICY_DECISION")
        self.assertEqual(snapshot["interaction_relationships"]["runtime-A-1"]["runtime-B-1"], 4)

    def test_audit_provider_is_explicit_and_validated(self):
        pipeline = FakePipeline(self.registry)
        no_audit = LiveDashboardSnapshotProvider(pipeline)()
        self.assertFalse(no_audit["audit_trail_configured"])
        self.assertEqual(no_audit["audit_trail"], [])

        with_audit = LiveDashboardSnapshotProvider(
            pipeline, audit_provider=lambda: [{"action": "TEST", "event_id": "evt-1"}]
        )()
        self.assertTrue(with_audit["audit_trail_configured"])
        self.assertEqual(with_audit["audit_trail"][0]["event_id"], "evt-1")

        invalid = LiveDashboardSnapshotProvider(pipeline, audit_provider=lambda: {"bad": "shape"})
        with self.assertRaises(TypeError):
            invalid()

    def test_registry_must_be_the_pipeline_registry(self):
        other_registry = AgentRegistry()
        pipeline = FakePipeline(self.registry)
        with self.assertRaisesRegex(ValueError, "same AgentRegistry"):
            LiveDashboardSnapshotProvider(pipeline, other_registry)

    def test_provider_reads_real_pipeline_and_registry_objects(self):
        pipeline = AnalysisPipeline(registry=self.registry)
        snapshot = LiveDashboardSnapshotProvider(pipeline, self.registry)()
        self.assertEqual(snapshot["status"], "ok")
        self.assertEqual(snapshot["agent_status"][0]["runtime_agent_id"], "runtime-A-1")
        self.assertEqual(snapshot["metrics"]["processed"], 0)
        json.dumps(snapshot)  # the HTTP dashboard payload must be serializable

    def test_invalid_pipeline_contract_is_rejected(self):
        with self.assertRaisesRegex(TypeError, "snapshot"):
            LiveDashboardSnapshotProvider(object(), self.registry)


if __name__ == "__main__":
    unittest.main()
