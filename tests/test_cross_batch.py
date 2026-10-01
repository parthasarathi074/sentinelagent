import unittest

from sentinelagent.cross_batch import CrossBatchAnalyzer


class TestCrossBatchAnalyzer(unittest.TestCase):
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
            },
        )


if __name__ == "__main__":
    unittest.main()
