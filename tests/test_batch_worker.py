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

    def test_pipeline_retries_batch_after_analysis_failure(self):
        pipeline = AnalysisPipeline()
        original_analyze = pipeline.analysis_engine.analyze
        attempts = 0

        def fail_once(batch):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("temporary analysis failure")
            return original_analyze(batch)

        pipeline.analysis_engine.analyze = fail_once
        batch = [
            {
                "event_id": "event-retry",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-B",
            }
        ]

        with self.assertRaisesRegex(
            RuntimeError,
            "temporary analysis failure",
        ):
            pipeline.process(batch, batch_id="batch-retry")

        result = pipeline.process(batch, batch_id="batch-retry")

        self.assertEqual(result["event_count"], 1)
        self.assertEqual(attempts, 2)
        snapshot = pipeline.snapshot()
        self.assertEqual(snapshot["batches_processed"], 1)
        self.assertEqual(
            snapshot["runtime_target_interactions"],
            {"runtime-A": {"runtime-B": 1}},
        )

    def test_cross_batch_retry_after_accumulation_failure(self):
        analyzer = CrossBatchAnalyzer()
        analysis_result = {
            "runtime_target_interactions": {
                "runtime-A": {"runtime-B": 1},
            }
        }
        original_counts = analyzer._interaction_counts

        class FailingCounts(dict):
            def setdefault(self, key, default=None):
                raise RuntimeError("simulated accumulation failure")

        analyzer._interaction_counts = FailingCounts()

        with self.assertRaisesRegex(
            RuntimeError,
            "simulated accumulation failure",
        ):
            analyzer.process_batch(
                analysis_result,
                batch_id="batch-accumulation-retry",
            )

        analyzer._interaction_counts = original_counts
        analyzer.process_batch(
            analysis_result,
            batch_id="batch-accumulation-retry",
        )
        snapshot = analyzer.snapshot()
        self.assertEqual(
            snapshot["runtime_target_interactions"],
            {"runtime-A": {"runtime-B": 1}},
        )
        self.assertEqual(snapshot["batches_processed"], 1)

    def test_cross_batch_failure_does_not_partially_accumulate(self):
        analyzer = CrossBatchAnalyzer()
        analysis_result = {
            "runtime_target_interactions": {
                "runtime-A": {"runtime-B": 1},
                "runtime-C": {"runtime-D": 1},
            }
        }

        class FailingCounts(dict):
            def setdefault(self, key, default=None):
                if key == "runtime-C":
                    raise RuntimeError("simulated partial accumulation failure")
                return super().setdefault(key, default)

        failing_counts = FailingCounts()
        analyzer._interaction_counts = failing_counts

        with self.assertRaisesRegex(
            RuntimeError,
            "simulated partial accumulation failure",
        ):
            analyzer.process_batch(
                analysis_result,
                batch_id="batch-partial-failure",
            )

        self.assertEqual(failing_counts, {})
        self.assertEqual(analyzer.snapshot()["batches_processed"], 0)

    def test_cross_batch_retry_after_partial_failure_is_idempotent(self):
        analyzer = CrossBatchAnalyzer()
        analysis_result = {
            "runtime_target_interactions": {
                "runtime-A": {"runtime-B": 1},
                "runtime-C": {"runtime-D": 1},
            }
        }

        class FailingCounts(dict):
            def setdefault(self, key, default=None):
                if key == "runtime-C":
                    raise RuntimeError(
                        "simulated partial accumulation failure"
                    )
                return super().setdefault(key, default)

        failing_counts = FailingCounts()
        analyzer._interaction_counts = failing_counts

        with self.assertRaisesRegex(
            RuntimeError,
            "simulated partial accumulation failure",
        ):
            analyzer.process_batch(
                analysis_result,
                batch_id="batch-partial-retry",
            )

        # The failed attempt must not modify the original state.
        self.assertEqual(failing_counts, {})
        self.assertEqual(analyzer.snapshot()["batches_processed"], 0)

        # Restore a normal accumulator and retry using the same batch ID.
        analyzer._interaction_counts = {}
        analyzer.process_batch(
            analysis_result,
            batch_id="batch-partial-retry",
        )
        analyzer.process_batch(
            analysis_result,
            batch_id="batch-partial-retry",
        )

        snapshot = analyzer.snapshot()
        self.assertEqual(snapshot["batches_processed"], 1)
        self.assertEqual(
            snapshot["runtime_target_interactions"],
            {
                "runtime-A": {"runtime-B": 1},
                "runtime-C": {"runtime-D": 1},
            },
        )

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

    def test_pipeline_retry_after_accumulation_failure_is_idempotent(self):
        pipeline = AnalysisPipeline()
        batch = [
            {
                "event_id": "event-retry-A",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-B",
            },
            {
                "event_id": "event-retry-C",
                "source_runtime_id": "runtime-C",
                "target_runtime_id": "runtime-D",
            },
        ]
        original_process_batch = (
            pipeline.cross_batch_analyzer.process_batch
        )
        attempts = 0

        def fail_once(analysis_result, batch_id=None):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("simulated accumulation failure")
            return original_process_batch(
                analysis_result,
                batch_id=batch_id,
            )

        pipeline.cross_batch_analyzer.process_batch = fail_once
        with self.assertRaisesRegex(
            RuntimeError,
            "simulated accumulation failure",
        ):
            pipeline.process(batch, batch_id="batch-pipeline-retry")

        # The failed accumulation must not record evidence.
        self.assertEqual(
            pipeline.snapshot()["batches_processed"],
            0,
        )

        # Retry the same batch ID through the complete pipeline.
        pipeline.process(batch, batch_id="batch-pipeline-retry")
        first_snapshot = pipeline.snapshot()

        # Replaying the successful batch must not change the snapshot.
        pipeline.process(batch, batch_id="batch-pipeline-retry")
        second_snapshot = pipeline.snapshot()
        self.assertEqual(first_snapshot["batches_processed"], 1)
        self.assertEqual(
            first_snapshot["runtime_target_interactions"],
            {
                "runtime-A": {"runtime-B": 1},
                "runtime-C": {"runtime-D": 1},
            },
        )
        self.assertEqual(second_snapshot, first_snapshot)

    def test_pipeline_reanalyzes_batch_after_accumulation_failure(self):
        pipeline = AnalysisPipeline()
        batch = [
            {
                "event_id": "event-reanalysis",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-B",
            }
        ]
        original_analyze = pipeline.analysis_engine.analyze
        analysis_calls = 0

        def track_analysis(events):
            nonlocal analysis_calls
            analysis_calls += 1
            return original_analyze(events)

        pipeline.analysis_engine.analyze = track_analysis
        original_process_batch = (
            pipeline.cross_batch_analyzer.process_batch
        )
        accumulation_calls = 0

        def fail_once(analysis_result, batch_id=None):
            nonlocal accumulation_calls
            accumulation_calls += 1
            if accumulation_calls == 1:
                raise RuntimeError("simulated accumulation failure")
            return original_process_batch(
                analysis_result,
                batch_id=batch_id,
            )

        pipeline.cross_batch_analyzer.process_batch = fail_once
        with self.assertRaisesRegex(
            RuntimeError,
            "simulated accumulation failure",
        ):
            pipeline.process(batch, batch_id="batch-reanalysis")

        self.assertEqual(analysis_calls, 1)
        self.assertEqual(
            pipeline.snapshot()["batches_processed"],
            0,
        )

        pipeline.process(batch, batch_id="batch-reanalysis")
        self.assertEqual(analysis_calls, 2)
        self.assertEqual(
            pipeline.snapshot()["batches_processed"],
            1,
        )
        self.assertEqual(
            pipeline.snapshot()["runtime_target_interactions"],
            {"runtime-A": {"runtime-B": 1}},
        )

    def test_failed_batch_does_not_block_other_batch_or_retry(self):
        pipeline = AnalysisPipeline()
        failed_batch = [
            {
                "event_id": "event-failed",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-B",
            }
        ]
        independent_batch = [
            {
                "event_id": "event-independent",
                "source_runtime_id": "runtime-C",
                "target_runtime_id": "runtime-D",
            }
        ]
        original_process_batch = (
            pipeline.cross_batch_analyzer.process_batch
        )
        should_fail = True

        def fail_once(analysis_result, batch_id=None):
            nonlocal should_fail
            if batch_id == "batch-failed" and should_fail:
                should_fail = False
                raise RuntimeError("simulated accumulation failure")
            return original_process_batch(
                analysis_result,
                batch_id=batch_id,
            )

        pipeline.cross_batch_analyzer.process_batch = fail_once
        with self.assertRaisesRegex(
            RuntimeError,
            "simulated accumulation failure",
        ):
            pipeline.process(failed_batch, batch_id="batch-failed")

        pipeline.process(
            independent_batch,
            batch_id="batch-independent",
        )
        pipeline.process(failed_batch, batch_id="batch-failed")

        snapshot = pipeline.snapshot()
        self.assertEqual(snapshot["batches_processed"], 2)
        self.assertEqual(
            snapshot["runtime_target_interactions"],
            {
                "runtime-A": {"runtime-B": 1},
                "runtime-C": {"runtime-D": 1},
            },
        )

    def test_analysis_failure_does_not_block_independent_batch_or_retry(self):
        pipeline = AnalysisPipeline()
        original_analyze = pipeline.analysis_engine.analyze
        should_fail = True

        def fail_once(batch):
            nonlocal should_fail
            if (
                batch[0].get("event_id") == "event-analysis-failed"
                and should_fail
            ):
                should_fail = False
                raise RuntimeError("simulated analysis failure")
            return original_analyze(batch)

        pipeline.analysis_engine.analyze = fail_once
        failed_batch = [
            {
                "event_id": "event-analysis-failed",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-B",
            }
        ]
        independent_batch = [
            {
                "event_id": "event-analysis-independent",
                "source_runtime_id": "runtime-C",
                "target_runtime_id": "runtime-D",
            }
        ]

        with self.assertRaisesRegex(
            RuntimeError,
            "simulated analysis failure",
        ):
            pipeline.process(
                failed_batch,
                batch_id="batch-analysis-failed",
            )

        self.assertEqual(pipeline.snapshot()["batches_processed"], 0)
        pipeline.process(
            independent_batch,
            batch_id="batch-analysis-independent",
        )
        pipeline.process(
            failed_batch,
            batch_id="batch-analysis-failed",
        )
        first_snapshot = pipeline.snapshot()
        pipeline.process(
            failed_batch,
            batch_id="batch-analysis-failed",
        )
        second_snapshot = pipeline.snapshot()

        self.assertEqual(first_snapshot["batches_processed"], 2)
        self.assertEqual(
            first_snapshot["runtime_target_interactions"],
            {
                "runtime-A": {"runtime-B": 1},
                "runtime-C": {"runtime-D": 1},
            },
        )
        self.assertEqual(second_snapshot, first_snapshot)


if __name__ == "__main__":
    unittest.main()
