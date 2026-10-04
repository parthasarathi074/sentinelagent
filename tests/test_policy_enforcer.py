import unittest

from sentinelagent.policy import PolicyAction, PolicyDecision
from sentinelagent.policy_enforcer import PolicyEnforcer
from sentinelagent.registry import AgentRegistry
from sentinelagent.models import AgentRecord, AgentStatus


class TestPolicyEnforcer(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = AgentRegistry()
        self.enforcer = PolicyEnforcer(self.registry)

    def _register_runtime(self, runtime_id: str) -> None:
        self.registry.register(
            AgentRecord(
                logical_agent_id=runtime_id,
                runtime_agent_id=runtime_id,
                role="test-agent",
                version="1.0",
                permissions=["read"],
                allowed_targets=[],
                status=AgentStatus.ACTIVE,
                credentials_reference=f"cred-{runtime_id}",
            )
        )

    def test_monitor_does_not_change_runtime_state(self) -> None:
        self._register_runtime("runtime-A")

        decisions = {
            "runtime-A": PolicyDecision(
                runtime_agent_id="runtime-A",
                risk_score=0,
                risk_level="LOW",
                action=PolicyAction.MONITOR,
                reasons=(),
            )
        }

        applied = self.enforcer.enforce(decisions)

        self.assertEqual(
            applied["runtime-A"],
            PolicyAction.MONITOR.value,
        )
        self.assertEqual(
            self.registry.get("runtime-A").status,
            AgentStatus.ACTIVE,
        )

    def test_medium_risk_marks_runtime_suspicious(self) -> None:
        self._register_runtime("runtime-A")

        decisions = {
            "runtime-A": PolicyDecision(
                runtime_agent_id="runtime-A",
                risk_score=20,
                risk_level="MEDIUM",
                action=PolicyAction.MARK_SUSPICIOUS,
                reasons=("REPEATED_TARGET_INTERACTIONS",),
            )
        }

        applied = self.enforcer.enforce(decisions)

        self.assertEqual(
            applied["runtime-A"],
            PolicyAction.MARK_SUSPICIOUS.value,
        )
        self.assertEqual(
            self.registry.get("runtime-A").status,
            AgentStatus.SUSPICIOUS,
        )

    def test_high_risk_quarantines_runtime(self) -> None:
        self._register_runtime("runtime-A")

        decisions = {
            "runtime-A": PolicyDecision(
                runtime_agent_id="runtime-A",
                risk_score=50,
                risk_level="HIGH",
                action=PolicyAction.QUARANTINE,
                reasons=("SHARED_TARGET_INTERACTIONS",),
            )
        }

        applied = self.enforcer.enforce(decisions)

        self.assertEqual(
            applied["runtime-A"],
            PolicyAction.QUARANTINE.value,
        )
        self.assertEqual(
            self.registry.get("runtime-A").status,
            AgentStatus.QUARANTINED,
        )

    def test_multiple_runtimes_are_enforced_independently(self) -> None:
        self._register_runtime("runtime-A")
        self._register_runtime("runtime-B")
        self._register_runtime("runtime-C")

        decisions = {
            "runtime-A": PolicyDecision(
                "runtime-A", 0, "LOW", PolicyAction.MONITOR, ()
            ),
            "runtime-B": PolicyDecision(
                "runtime-B",
                20,
                "MEDIUM",
                PolicyAction.MARK_SUSPICIOUS,
                ("REPEATED_TARGET_INTERACTIONS",),
            ),
            "runtime-C": PolicyDecision(
                "runtime-C",
                50,
                "HIGH",
                PolicyAction.QUARANTINE,
                ("SHARED_TARGET_INTERACTIONS",),
            ),
        }

        applied = self.enforcer.enforce(decisions)

        self.assertEqual(
            self.registry.get("runtime-A").status,
            AgentStatus.ACTIVE,
        )
        self.assertEqual(
            self.registry.get("runtime-B").status,
            AgentStatus.SUSPICIOUS,
        )
        self.assertEqual(
            self.registry.get("runtime-C").status,
            AgentStatus.QUARANTINED,
        )
        self.assertEqual(len(applied), 3)


if __name__ == "__main__":
    unittest.main()
