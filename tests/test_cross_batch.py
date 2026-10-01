import unittest

from sentinelagent.cross_batch import CrossBatchAnalyzer


class TestCrossBatchAnalyzer(unittest.TestCase):
    def test_detects_shared_target_across_batches(self):
        analyzer = CrossBatchAnalyzer()
        analyzer.process_batch({
            "runtime_target_interactions": {
                "runtime-A": {"runtime-C": 1}
            }
        })
        analyzer.process_batch({
            "runtime_target_interactions": {
                "runtime-B": {"runtime-C": 1}
            }
        })
        analyzer.process_batch({
            "runtime_target_interactions": {
                "runtime-A": {"runtime-C": 1}
            }
        })
        analyzer.process_batch({
            "runtime_target_interactions": {
                "runtime-B": {"runtime-C": 1}
            }
        })

        result = analyzer.snapshot()

        self.assertEqual(
            result["shared_target_interactions"],
            {
                "runtime-C": {
                    "runtime-A": 2,
                    "runtime-B": 2,
                }
            },
        )

    def test_assesses_medium_risk_for_repeated_interactions_across_batches(self):
        analyzer = CrossBatchAnalyzer()
        analyzer.process_batch({
            "runtime_target_interactions": {
                "runtime-A": {"runtime-B": 1}
            }
        })
        analyzer.process_batch({
            "runtime_target_interactions": {
                "runtime-A": {"runtime-B": 1}
            }
        })

        result = analyzer.snapshot()
        assessment = result["runtime_risk_assessments"]["runtime-A"]

        self.assertEqual(assessment["risk_score"], 20)
        self.assertEqual(assessment["risk_level"], "MEDIUM")
        self.assertIn(
            "REPEATED_TARGET_INTERACTIONS",
            assessment["reasons"],
        )

    def test_assesses_high_risk_for_repeated_shared_target_across_batches(self):
        analyzer = CrossBatchAnalyzer()
        for _ in range(2):
            analyzer.process_batch({
                "runtime_target_interactions": {
                    "runtime-A": {"runtime-C": 1},
                    "runtime-B": {"runtime-C": 1},
                }
            })

        result = analyzer.snapshot()

        for runtime_id in ("runtime-A", "runtime-B"):
            assessment = result["runtime_risk_assessments"][runtime_id]
            self.assertEqual(assessment["risk_score"], 50)
            self.assertEqual(assessment["risk_level"], "HIGH")
            self.assertIn(
                "REPEATED_TARGET_INTERACTIONS",
                assessment["reasons"],
            )
            self.assertIn(
                "SHARED_TARGET_INTERACTIONS",
                assessment["reasons"],
            )

    def test_does_not_flag_target_used_by_only_one_runtime(self):
        analyzer = CrossBatchAnalyzer()
        analyzer.process_batch({
            "runtime_target_interactions": {
                "runtime-A": {"runtime-C": 3}
            }
        })

        result = analyzer.snapshot()

        self.assertEqual(
            result["shared_target_interactions"],
            {},
        )

    def test_detects_repeated_interaction_across_batches(self):
        analyzer = CrossBatchAnalyzer()
        analyzer.process_batch({
            "runtime_target_interactions": {
                "runtime-A": {"runtime-B": 1}
            }
        })
        analyzer.process_batch({
            "runtime_target_interactions": {
                "runtime-A": {"runtime-B": 1}
            }
        })

        result = analyzer.snapshot()

        self.assertEqual(result["batches_processed"], 2)
        self.assertEqual(
            result["runtime_target_interactions"],
            {"runtime-A": {"runtime-B": 2}},
        )
        self.assertEqual(
            result["repeated_runtime_interactions"],
            {"runtime-A": {"runtime-B": 2}},
        )

    def test_accumulates_interactions_across_batches(self):
        analyzer = CrossBatchAnalyzer()

        analyzer.process_batch({
            "runtime_target_interactions": {
                "runtime-A": {"runtime-B": 1},
            },
        })
        analyzer.process_batch({
            "runtime_target_interactions": {
                "runtime-A": {"runtime-B": 1, "runtime-C": 1},
                "runtime-B": {"runtime-C": 1},
            },
        })

        self.assertEqual(
            analyzer.snapshot(),
            {
                "batches_processed": 2,
                "runtime_target_interactions": {
                    "runtime-A": {"runtime-B": 2, "runtime-C": 1},
                    "runtime-B": {"runtime-C": 1},
                },
                "repeated_runtime_interactions": {
                    "runtime-A": {"runtime-B": 2},
                },
                "shared_target_interactions": {
                    "runtime-C": {
                        "runtime-A": 1,
                        "runtime-B": 1,
                    },
                },
                "runtime_risk_assessments": {
                    "runtime-A": {
                        "risk_score": 50,
                        "risk_level": "HIGH",
                        "reasons": [
                            "REPEATED_TARGET_INTERACTIONS",
                            "SHARED_TARGET_INTERACTIONS",
                        ],
                    },
                    "runtime-B": {
                        "risk_score": 30,
                        "risk_level": "MEDIUM",
                        "reasons": ["SHARED_TARGET_INTERACTIONS"],
                    },
                },
            },
        )

    def test_snapshot_is_independent_of_accumulated_state(self):
        analyzer = CrossBatchAnalyzer()
        analyzer.process_batch({
            "runtime_target_interactions": {
                "runtime-A": {"runtime-B": 2},
            },
        })

        snapshot = analyzer.snapshot()
        snapshot["runtime_target_interactions"]["runtime-A"]["runtime-B"] = 10
        snapshot["repeated_runtime_interactions"]["runtime-A"]["runtime-B"] = 10

        self.assertEqual(
            analyzer.snapshot()["runtime_target_interactions"],
            {"runtime-A": {"runtime-B": 2}},
        )

    def test_ignores_invalid_interaction_entries(self):
        analyzer = CrossBatchAnalyzer()
        analyzer.process_batch({
            "runtime_target_interactions": {
                "": {"runtime-B": 1},
                "runtime-A": {
                    "": 1,
                    "runtime-B": True,
                    "runtime-C": -1,
                    "runtime-D": 2,
                },
                "runtime-B": [],
            },
        })

        self.assertEqual(
            analyzer.snapshot(),
            {
                "batches_processed": 1,
                "runtime_target_interactions": {
                    "runtime-A": {"runtime-D": 2},
                },
                "repeated_runtime_interactions": {
                    "runtime-A": {"runtime-D": 2},
                },
                "shared_target_interactions": {},
                "runtime_risk_assessments": {
                    "runtime-A": {
                        "risk_score": 20,
                        "risk_level": "MEDIUM",
                        "reasons": ["REPEATED_TARGET_INTERACTIONS"],
                    },
                },
            },
        )


if __name__ == "__main__":
    unittest.main()
