import unittest

from sentinelagent.policy import PolicyAction, PolicyEngine


class TestPolicyEngine(unittest.TestCase):

    def setUp(self) -> None:
        self.engine = PolicyEngine()

    def test_low_risk_maps_to_monitor(self) -> None:
        decisions = self.engine.evaluate(
            {
                "runtime-A": {
                    "risk_score": 0,
                    "risk_level": "LOW",
                    "reasons": [],
                }
            }
        )

        decision = decisions["runtime-A"]

        self.assertEqual(decision.action, PolicyAction.MONITOR)
        self.assertEqual(decision.risk_score, 0)
        self.assertEqual(decision.risk_level, "LOW")

    def test_medium_risk_maps_to_mark_suspicious(self) -> None:
        decisions = self.engine.evaluate(
            {
                "runtime-A": {
                    "risk_score": 20,
                    "risk_level": "MEDIUM",
                    "reasons": ["REPEATED_TARGET_INTERACTIONS"],
                }
            }
        )

        decision = decisions["runtime-A"]

        self.assertEqual(
            decision.action,
            PolicyAction.MARK_SUSPICIOUS,
        )
        self.assertEqual(
            decision.reasons,
            ("REPEATED_TARGET_INTERACTIONS",),
        )

    def test_high_risk_maps_to_quarantine(self) -> None:
        decisions = self.engine.evaluate(
            {
                "runtime-A": {
                    "risk_score": 50,
                    "risk_level": "HIGH",
                    "reasons": [
                        "REPEATED_TARGET_INTERACTIONS",
                        "SHARED_TARGET_INTERACTIONS",
                    ],
                }
            }
        )

        decision = decisions["runtime-A"]

        self.assertEqual(decision.action, PolicyAction.QUARANTINE)
        self.assertEqual(decision.risk_score, 50)

    def test_multiple_runtimes_are_evaluated_independently(self) -> None:
        decisions = self.engine.evaluate(
            {
                "runtime-A": {
                    "risk_score": 0,
                    "risk_level": "LOW",
                    "reasons": [],
                },
                "runtime-B": {
                    "risk_score": 20,
                    "risk_level": "MEDIUM",
                    "reasons": ["REPEATED_TARGET_INTERACTIONS"],
                },
                "runtime-C": {
                    "risk_score": 50,
                    "risk_level": "HIGH",
                    "reasons": ["SHARED_TARGET_INTERACTIONS"],
                },
            }
        )

        self.assertEqual(
            decisions["runtime-A"].action,
            PolicyAction.MONITOR,
        )
        self.assertEqual(
            decisions["runtime-B"].action,
            PolicyAction.MARK_SUSPICIOUS,
        )
        self.assertEqual(
            decisions["runtime-C"].action,
            PolicyAction.QUARANTINE,
        )

    def test_invalid_risk_level_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.engine.evaluate(
                {
                    "runtime-A": {
                        "risk_score": 10,
                        "risk_level": "UNKNOWN",
                        "reasons": [],
                    }
                }
            )

    def test_invalid_risk_score_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.engine.evaluate(
                {
                    "runtime-A": {
                        "risk_score": -1,
                        "risk_level": "LOW",
                        "reasons": [],
                    }
                }
            )

    def test_analysis_result_can_be_evaluated_directly(self) -> None:
        decisions = self.engine.evaluate_analysis(
            {
                "runtime_risk_assessments": {
                    "runtime-A": {
                        "risk_score": 50,
                        "risk_level": "HIGH",
                        "reasons": ["SHARED_TARGET_INTERACTIONS"],
                    }
                }
            }
        )

        self.assertEqual(
            decisions["runtime-A"].action,
            PolicyAction.QUARANTINE,
        )

    def test_policy_decision_to_dict_is_json_safe(self):
        import json

        result = self.engine.evaluate(
            {
                "runtime-a": {
                    "risk_score": 60,
                    "risk_level": "HIGH",
                    "reasons": ["REPEATED_TARGET_INTERACTIONS"],
                }
            }
        )

        decision = result["runtime-a"]
        payload = decision.to_dict()

        encoded = json.dumps(payload)

        self.assertIsInstance(encoded, str)
        self.assertEqual(payload["runtime_agent_id"], "runtime-a")
        self.assertEqual(payload["action"], "QUARANTINE")
        self.assertIsInstance(payload["reasons"], list)

if __name__ == "__main__":
    unittest.main()
