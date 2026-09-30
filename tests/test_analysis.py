import unittest

from sentinelagent.analysis import AnalysisEngine


class TestAnalysisEngine(unittest.TestCase):
    def test_analyze_counts_events(self):
        engine = AnalysisEngine()

        batch = [
            {"event_id": "event-1"},
            {"event_id": "event-2"},
            {"event_id": "event-3"},
        ]

        result = engine.analyze(batch)

        self.assertEqual(result["event_count"], 3)

    def test_analyze_returns_event_ids(self):
        engine = AnalysisEngine()

        batch = [
            {"event_id": "event-1"},
            {"event_id": "event-2"},
        ]

        result = engine.analyze(batch)

        self.assertEqual(
            result["event_ids"],
            ["event-1", "event-2"],
        )

    def test_analyze_empty_batch(self):
        engine = AnalysisEngine()

        result = engine.analyze([])

        self.assertEqual(result["event_count"], 0)
        self.assertEqual(result["event_ids"], [])


if __name__ == "__main__":
    unittest.main()