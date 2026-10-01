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

    def test_analyze_counts_events_by_source_runtime(self):
        engine = AnalysisEngine()

        batch = [
            {
                "event_id": "event-1",
                "source_runtime_id": "runtime-A",
            },
            {
                "event_id": "event-2",
                "source_runtime_id": "runtime-B",
            },
            {
                "event_id": "event-3",
                "source_runtime_id": "runtime-A",
            },
            {
                "event_id": "event-4",
                "source_runtime_id": "runtime-A",
            },
        ]

        result = engine.analyze(batch)

        self.assertEqual(
            result["events_by_source_runtime"],
            {
                "runtime-A": 3,
                "runtime-B": 1,
            },
        )

    def test_analyze_tracks_runtime_activity_window(self):
        engine = AnalysisEngine()

        batch = [
            {
                "event_id": "event-1",
                "source_runtime_id": "runtime-A",
                "timestamp": "2026-01-01T10:00:01Z",
            },
            {
                "event_id": "event-2",
                "source_runtime_id": "runtime-A",
                "timestamp": "2026-01-01T10:00:03Z",
            },
            {
                "event_id": "event-3",
                "source_runtime_id": "runtime-B",
                "timestamp": "2026-01-01T10:00:02Z",
            },
            {
                "event_id": "event-4",
                "source_runtime_id": "runtime-A",
                "timestamp": "2026-01-01T10:00:05Z",
            },
        ]

        result = engine.analyze(batch)

        self.assertEqual(
            result["runtime_activity_windows"],
            {
                "runtime-A": {
                    "first_event_timestamp": "2026-01-01T10:00:01Z",
                    "last_event_timestamp": "2026-01-01T10:00:05Z",
                },
                "runtime-B": {
                    "first_event_timestamp": "2026-01-01T10:00:02Z",
                    "last_event_timestamp": "2026-01-01T10:00:02Z",
                },
            },
        )

    def test_analyze_calculates_runtime_event_rate(self):
        engine = AnalysisEngine()

        batch = [
            {
                "event_id": "event-1",
                "source_runtime_id": "runtime-A",
                "timestamp": "2026-01-01T10:00:01Z",
            },
            {
                "event_id": "event-2",
                "source_runtime_id": "runtime-A",
                "timestamp": "2026-01-01T10:00:03Z",
            },
            {
                "event_id": "event-3",
                "source_runtime_id": "runtime-A",
                "timestamp": "2026-01-01T10:00:05Z",
            },
            {
                "event_id": "event-4",
                "source_runtime_id": "runtime-B",
                "timestamp": "2026-01-01T10:00:02Z",
            },
        ]

        result = engine.analyze(batch)

        self.assertEqual(
            result["runtime_event_rates"],
            {
                "runtime-A": 0.75,
                "runtime-B": 0.0,
            },
        )

    def test_analyze_tracks_runtime_target_interactions(self):
        engine = AnalysisEngine()

        batch = [
            {
                "event_id": "event-1",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-B",
            },
            {
                "event_id": "event-2",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-B",
            },
            {
                "event_id": "event-3",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-C",
            },
            {
                "event_id": "event-4",
                "source_runtime_id": "runtime-B",
                "target_runtime_id": "runtime-C",
            },
        ]

        result = engine.analyze(batch)

        self.assertEqual(
            result["runtime_target_interactions"],
            {
                "runtime-A": {
                    "runtime-B": 2,
                    "runtime-C": 1,
                },
                "runtime-B": {
                    "runtime-C": 1,
                },
            },
        )

    def test_analyze_identifies_repeated_runtime_interactions(self):
        engine = AnalysisEngine()

        batch = [
            {
                "event_id": "event-1",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-B",
            },
            {
                "event_id": "event-2",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-B",
            },
            {
                "event_id": "event-3",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-C",
            },
            {
                "event_id": "event-4",
                "source_runtime_id": "runtime-B",
                "target_runtime_id": "runtime-C",
            },
            {
                "event_id": "event-5",
                "source_runtime_id": "runtime-B",
                "target_runtime_id": "runtime-C",
            },
        ]

        result = engine.analyze(batch)

        self.assertEqual(
            result["repeated_runtime_interactions"],
            {
                "runtime-A": {
                    "runtime-B": 2,
                },
                "runtime-B": {
                    "runtime-C": 2,
                },
            },
        )

    def test_analyze_identifies_shared_targets_across_runtimes(self):
        engine = AnalysisEngine()

        batch = [
            {
                "event_id": "event-1",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-C",
            },
            {
                "event_id": "event-2",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-C",
            },
            {
                "event_id": "event-3",
                "source_runtime_id": "runtime-B",
                "target_runtime_id": "runtime-C",
            },
            {
                "event_id": "event-4",
                "source_runtime_id": "runtime-B",
                "target_runtime_id": "runtime-C",
            },
            {
                "event_id": "event-5",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-D",
            },
        ]

        result = engine.analyze(batch)

        self.assertEqual(
            result["shared_target_interactions"],
            {
                "runtime-C": {
                    "runtime-A": 2,
                    "runtime-B": 2,
                },
            },
        )

    def test_shared_target_requires_repeated_interactions_from_each_source(self):
        engine = AnalysisEngine()

        batch = [
            {
                "event_id": "event-1",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-C",
            },
            {
                "event_id": "event-2",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-C",
            },
            {
                "event_id": "event-3",
                "source_runtime_id": "runtime-B",
                "target_runtime_id": "runtime-C",
            },
        ]

        result = engine.analyze(batch)

        self.assertEqual(
            result["shared_target_interactions"],
            {},
        )

    def test_analyze_empty_batch(self):
        engine = AnalysisEngine()

        result = engine.analyze([])

        self.assertEqual(result["event_count"], 0)
        self.assertEqual(result["event_ids"], [])


if __name__ == "__main__":
    unittest.main()