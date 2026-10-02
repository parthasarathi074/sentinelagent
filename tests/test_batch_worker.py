import unittest

from sentinelagent.analysis import AnalysisEngine
from sentinelagent.analysis_pipeline import AnalysisPipeline
from sentinelagent.batch_worker import BatchWorker
from sentinelagent.cross_batch import CrossBatchAnalyzer


class TestBatchWorker(unittest.TestCase):

    def test_processes_batch_with_analysis_handler(self):
        processed_batches = []

        def analysis_handler(batch):
            processed_batches.append(batch)
            return "analysis-complete"

        worker = BatchWorker(
            batch_handler=analysis_handler,
        )

        batch = [
            {"event_id": "event-1"},
            {"event_id": "event-2"},
        ]

        result = worker.process(batch)

        self.assertEqual(result, "analysis-complete")
        self.assertEqual(processed_batches, [batch])

    def test_batch_worker_can_use_analysis_engine(self):
        engine = AnalysisEngine()

        worker = BatchWorker(
            batch_handler=engine.analyze,
        )

        batch = [
            {"event_id": "event-101"},
            {"event_id": "event-102"},
            {"event_id": "event-103"},
        ]

        result = worker.process(batch)

        self.assertEqual(result["event_count"], 3)
        self.assertEqual(
            result["event_ids"],
            ["event-101", "event-102", "event-103"],
        )

    def test_empty_batch_is_rejected(self):
        worker = BatchWorker(
            batch_handler=lambda batch: None,
        )

        with self.assertRaises(ValueError):
            worker.process([])

    def test_handler_receives_original_batch(self):
        received_batch = []

        def analysis_handler(batch):
            received_batch.extend(batch)

        worker = BatchWorker(
            batch_handler=analysis_handler,
        )

        batch = [
            {"event_id": "event-1"},
            {"event_id": "event-2"},
        ]

        worker.process(batch)

        self.assertEqual(received_batch, batch)

    def test_micro_batch_reaches_batch_worker_and_analysis_handler(self):
        from sentinelagent.event_stream import EventStream
        from sentinelagent.micro_batch import MicroBatchProcessor

        stream = EventStream()
        processed_batches = []

        def analysis_handler(batch):
            processed_batches.append(batch)

        worker = BatchWorker(
            batch_handler=analysis_handler,
        )

        processor = MicroBatchProcessor(
            event_stream=stream,
            batch_size=2,
            batch_handler=worker.process,
        )

        stream.publish({"event_id": "event-1"})
        stream.publish({"event_id": "event-2"})

        result = processor.process_once()

        expected_batch = [
            {"event_id": "event-1"},
            {"event_id": "event-2"},
        ]

        self.assertEqual(result, expected_batch)
        self.assertEqual(processed_batches, [expected_batch])
        self.assertEqual(stream.size(), 0)

    def test_pipeline_accumulates_analysis_across_batches(self):
        engine = AnalysisEngine()
        cross_batch = CrossBatchAnalyzer()

        def analysis_handler(batch, batch_id=None):
            analysis_result = engine.analyze(batch)
            cross_batch.process_batch(
                analysis_result,
                batch_id=batch_id,
            )
            return analysis_result

        worker = BatchWorker(batch_handler=analysis_handler)
        first_batch = [
            {
                "event_id": "event-1",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-B",
            }
        ]
        second_batch = [
            {
                "event_id": "event-2",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-B",
            }
        ]

        worker.process(first_batch, batch_id="batch-001")
        worker.process(second_batch, batch_id="batch-002")
        worker.process(second_batch, batch_id="batch-002")

        result = cross_batch.snapshot()
        self.assertEqual(result["batches_processed"], 2)
        self.assertEqual(
            result["runtime_target_interactions"],
            {"runtime-A": {"runtime-B": 2}},
        )
        self.assertEqual(
            result["repeated_runtime_interactions"],
            {"runtime-A": {"runtime-B": 2}},
        )

    def test_analysis_pipeline_integrates_all_components(self):
        pipeline = AnalysisPipeline()
        first_batch = [
            {
                "event_id": "event-1",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-B",
            }
        ]
        second_batch = [
            {
                "event_id": "event-2",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-B",
            }
        ]

        first_result = pipeline.process(
            first_batch,
            batch_id="batch-001",
        )
        second_result = pipeline.process(
            second_batch,
            batch_id="batch-002",
        )
        pipeline.process(
            second_batch,
            batch_id="batch-002",
        )

        self.assertEqual(first_result["event_count"], 1)
        self.assertEqual(second_result["event_count"], 1)
        snapshot = pipeline.snapshot()
        self.assertEqual(snapshot["batches_processed"], 2)
        self.assertEqual(
            snapshot["runtime_target_interactions"],
            {"runtime-A": {"runtime-B": 2}},
        )

    def test_pipeline_analysis_failure_does_not_update_snapshot(self):
        pipeline = AnalysisPipeline()
        initial_snapshot = pipeline.snapshot()

        def failing_analysis(batch):
            raise RuntimeError("simulated analysis failure")

        pipeline.analysis_engine.analyze = failing_analysis
        batch = [
            {
                "event_id": "event-failure",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-B",
            }
        ]

        with self.assertRaisesRegex(
            RuntimeError,
            "simulated analysis failure",
        ):
            pipeline.process(batch, batch_id="batch-failure")

        self.assertEqual(pipeline.snapshot(), initial_snapshot)

    def test_pipeline_duplicate_batch_does_not_double_count(self):
        pipeline = AnalysisPipeline()
        batch = [
            {
                "event_id": "event-duplicate",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-B",
            }
        ]

        pipeline.process(batch, batch_id="batch-duplicate")
        first_snapshot = pipeline.snapshot()
        pipeline.process(batch, batch_id="batch-duplicate")
        second_snapshot = pipeline.snapshot()

        self.assertEqual(second_snapshot, first_snapshot)


if __name__ == "__main__":
    unittest.main()
