import logging
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

        worker = BatchWorker(batch_handler=analysis_handler)

        batch = [
            {"event_id": "event-1"},
            {"event_id": "event-2"},
        ]

        result = worker.process(batch)

        self.assertEqual(result, "analysis-complete")
        self.assertEqual(processed_batches, [batch])

    def test_batch_worker_can_use_analysis_engine(self):
        engine = AnalysisEngine()
        worker = BatchWorker(batch_handler=engine.analyze)

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
        worker = BatchWorker(batch_handler=lambda batch: None)

        with self.assertRaises(ValueError):
            worker.process([])

    def test_handler_receives_original_batch(self):
        received_batch = []

        def analysis_handler(batch):
            received_batch.extend(batch)

        worker = BatchWorker(batch_handler=analysis_handler)

        batch = [
            {"event_id": "event-1"},
            {"event_id": "event-2"},
        ]

        worker.process(batch)

        self.assertEqual(received_batch, batch)

    def test_successful_batch_emits_structured_log(self):
        def analysis_handler(batch, batch_id=None):
            return "analysis-complete"

        worker = BatchWorker(batch_handler=analysis_handler)

        with self.assertLogs(
            "sentinelagent.batch_worker",
            level=logging.INFO,
        ) as captured:
            result = worker.process(
                [{"event_id": "event-1"}, {"event_id": "event-2"}],
                batch_id="batch-001",
            )

        self.assertEqual(result, "analysis-complete")
        self.assertTrue(
            any("batch-001" in message for message in captured.output)
        )
        self.assertTrue(
            any("event_count=2" in message for message in captured.output)
        )
        self.assertTrue(
            any("status=success" in message for message in captured.output)
        )

    def test_failed_batch_emits_error_log_and_reraises(self):
        def failing_handler(batch, batch_id=None):
            raise RuntimeError("simulated failure")

        worker = BatchWorker(batch_handler=failing_handler)

        with self.assertLogs(
            "sentinelagent.batch_worker",
            level=logging.ERROR,
        ) as captured:
            with self.assertRaisesRegex(
                RuntimeError,
                "simulated failure",
            ):
                worker.process(
                    [{"event_id": "event-failure"}],
                    batch_id="batch-failure",
                )

        self.assertTrue(
            any("batch-failure" in message for message in captured.output)
        )
        self.assertTrue(
            any("status=failure" in message for message in captured.output)
        )

    def test_micro_batch_reaches_batch_worker_and_analysis_handler(self):
        from sentinelagent.event_stream import EventStream
        from sentinelagent.micro_batch import MicroBatchProcessor

        stream = EventStream()
        processed_batches = []

        def analysis_handler(batch):
            processed_batches.append(batch)

        worker = BatchWorker(batch_handler=analysis_handler)

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

    def test_edge_event_reaches_analysis_through_micro_batch(self):
        from sentinelagent.edge import SentinelEdge
        from sentinelagent.event_stream import EventStream
        from sentinelagent.micro_batch import MicroBatchProcessor
        from sentinelagent.models import AgentRecord, EdgeDecision, EdgeRequest
        from sentinelagent.registry import AgentRegistry

        registry = AgentRegistry()
        registry.register(
            AgentRecord(
                logical_agent_id="agent-A",
                runtime_agent_id="runtime-A-001",
                role="ingest",
                version="1.0",
                permissions={"message:send"},
                allowed_targets={"agent-B"},
            )
        )
        registry.register(
            AgentRecord(
                logical_agent_id="agent-B",
                runtime_agent_id="runtime-B-001",
                role="validator",
                version="1.0",
                permissions={"validation:execute"},
                allowed_targets={"agent-A"},
            )
        )
        registry.activate("runtime-A-001")
        registry.activate("runtime-B-001")

        stream = EventStream()
        engine = AnalysisEngine()
        analysis_results = []
        worker = BatchWorker(
            batch_handler=lambda batch: analysis_results.append(
                engine.analyze(batch)
            )
        )
        processor = MicroBatchProcessor(
            event_stream=stream,
            batch_size=1,
            batch_handler=worker.process,
        )
        edge = SentinelEdge(registry=registry, event_sink=stream.publish)
        request = EdgeRequest(
            request_id="request-e2e-001",
            trace_id="trace-e2e-001",
            edge_id="edge-agent-A-agent-B",
            source_runtime_id="runtime-A-001",
            target_runtime_id="runtime-B-001",
            action="SEND_MESSAGE",
            requested_permission="message:send",
        )

        edge_result = edge.handle(request)
        processed_batch = processor.process_once()

        self.assertEqual(edge_result.decision, EdgeDecision.ALLOW)
        self.assertEqual(len(processed_batch), 1)
        self.assertEqual(stream.size(), 0)
        self.assertEqual(len(analysis_results), 1)
        self.assertEqual(analysis_results[0]["event_count"], 1)
        self.assertEqual(analysis_results[0]["invalid_events"], 0)
        self.assertEqual(
            analysis_results[0]["events_by_source_runtime"],
            {"runtime-A-001": 1},
        )
        self.assertEqual(
            analysis_results[0]["runtime_target_interactions"],
            {"runtime-A-001": {"runtime-B-001": 1}},
        )

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

        first_result = pipeline.process(first_batch, batch_id="batch-001")
        second_result = pipeline.process(second_batch, batch_id="batch-002")

        pipeline.process(second_batch, batch_id="batch-002")

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

        self.assertEqual(failing_counts, {})
        self.assertEqual(analyzer.snapshot()["batches_processed"], 0)

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

    def test_pipeline_rejects_batch_id_reuse_with_different_content(self):
        pipeline = AnalysisPipeline()

        first_batch = [
            {
                "event_id": "event-first",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-B",
            }
        ]

        different_batch = [
            {
                "event_id": "event-different",
                "source_runtime_id": "runtime-C",
                "target_runtime_id": "runtime-D",
            }
        ]

        pipeline.process(first_batch, batch_id="batch-reused")
        first_snapshot = pipeline.snapshot()

        with self.assertRaisesRegex(
            ValueError,
            "batch_id.*different content",
        ):
            pipeline.process(
                different_batch,
                batch_id="batch-reused",
            )

        self.assertEqual(pipeline.snapshot(), first_snapshot)

    def test_pipeline_retry_after_policy_failure_does_not_double_count(self):
        pipeline = AnalysisPipeline()
        batch = [
            {
                "event_id": "event-policy-retry",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-B",
            }
        ]

        original_evaluate = pipeline.policy_engine.evaluate_analysis
        attempts = 0

        def fail_once(analysis_result):
            nonlocal attempts
            attempts += 1

            if attempts == 1:
                raise RuntimeError("simulated policy failure")

            return original_evaluate(analysis_result)

        pipeline.policy_engine.evaluate_analysis = fail_once

        with self.assertRaisesRegex(
            RuntimeError,
            "simulated policy failure",
        ):
            pipeline.process(batch, batch_id="batch-policy-retry")

        failed_snapshot = pipeline.snapshot()
        self.assertEqual(failed_snapshot["batches_processed"], 1)
        self.assertEqual(
            failed_snapshot["runtime_target_interactions"],
            {"runtime-A": {"runtime-B": 1}},
        )

        pipeline.process(batch, batch_id="batch-policy-retry")
        successful_snapshot = pipeline.snapshot()

        self.assertEqual(successful_snapshot["batches_processed"], 1)
        self.assertEqual(
            successful_snapshot["runtime_target_interactions"],
            {"runtime-A": {"runtime-B": 1}},
        )
        self.assertEqual(attempts, 2)

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

        original_process_batch = pipeline.cross_batch_analyzer.process_batch
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

        self.assertEqual(pipeline.snapshot()["batches_processed"], 0)

        pipeline.process(batch, batch_id="batch-pipeline-retry")
        first_snapshot = pipeline.snapshot()

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
        original_process_batch = pipeline.cross_batch_analyzer.process_batch
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
        self.assertEqual(pipeline.snapshot()["batches_processed"], 0)

        pipeline.process(batch, batch_id="batch-reanalysis")

        self.assertEqual(analysis_calls, 2)
        self.assertEqual(pipeline.snapshot()["batches_processed"], 1)
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

        original_process_batch = pipeline.cross_batch_analyzer.process_batch
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

    def test_analysis_failure_does_not_block_independent_batch_or_retry(
        self,
    ):
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

    def test_pipeline_logs_policy_decision_with_batch_and_runtime_ids(self):
        pipeline = AnalysisPipeline()
        batch = [
            {
                "event_id": "observability-event-1",
                "source_runtime_id": "runtime-observed-A",
                "target_runtime_id": "runtime-observed-B",
            }
        ]

        with self.assertLogs(
            "sentinelagent.analysis_pipeline",
            level=logging.INFO,
        ) as captured:
            pipeline.process(batch, batch_id="observability-batch-1")

        message = "\n".join(captured.output)
        self.assertIn("sentinel_policy_decision", message)
        self.assertIn("observability-batch-1", message)
        self.assertIn("runtime-observed-A", message)
        self.assertIn("risk_level=LOW", message)
        self.assertIn("action=MONITOR", message)

    def test_pipeline_policy_decisions_are_json_safe(self):
        import json

        pipeline = AnalysisPipeline()

        result = pipeline.process(
            [
                {
                    "event_id": "json-safe-event",
                    "source_runtime_id": "runtime-a",
                    "target_runtime_id": "runtime-b",
                    "timestamp": "2026-01-01T00:00:00Z",
                }
            ],
            batch_id="json-safe-batch",
        )

        encoded = json.dumps(result["policy_decisions_json"])
        self.assertIsInstance(encoded, str)

        decision = result["policy_decisions_json"]["runtime-a"]

        self.assertIsInstance(decision["action"], str)
        self.assertIsInstance(decision["reasons"], list)

    def test_pipeline_snapshot_is_read_only_for_enforcement(self):
        from sentinelagent.models import AgentRecord, AgentStatus
        from sentinelagent.registry import AgentRegistry

        registry = AgentRegistry()

        registry.register(
            AgentRecord(
                runtime_agent_id="runtime-a",
                logical_agent_id="agent-a",
                role="worker",
                version="1",
                permissions={"read"},
                allowed_targets={"agent-b"},
                status=AgentStatus.ACTIVE,
            )
        )
        registry.register(
            AgentRecord(
                runtime_agent_id="runtime-b",
                logical_agent_id="agent-b",
                role="worker",
                version="1",
                permissions={"read"},
                allowed_targets=set(),
                status=AgentStatus.ACTIVE,
            )
        )

        pipeline = AnalysisPipeline(registry=registry)

        first = [
            {
                "event_id": "snapshot-1",
                "source_runtime_id": "runtime-a",
                "target_runtime_id": "runtime-b",
                "timestamp": "2026-01-01T00:00:00Z",
            },
            {
                "event_id": "snapshot-2",
                "source_runtime_id": "runtime-a",
                "target_runtime_id": "runtime-b",
                "timestamp": "2026-01-01T00:00:01Z",
            },
        ]

        pipeline.process(first, batch_id="snapshot-batch-1")

        status_before_snapshot = registry.get("runtime-a").status
        snapshot = pipeline.snapshot()
        status_after_snapshot = registry.get("runtime-a").status

        self.assertIn("policy_decisions", snapshot)
        self.assertIn("policy_decisions_json", snapshot)
        self.assertEqual(status_after_snapshot, status_before_snapshot)
        self.assertNotIn("enforced_actions", snapshot)

    def test_pipeline_exposes_replacement_runtime_ids(self):
        from sentinelagent.models import AgentRecord, AgentStatus
        from sentinelagent.registry import AgentRegistry

        registry = AgentRegistry()

        registry.register(
            AgentRecord(
                runtime_agent_id="runtime-a",
                logical_agent_id="agent-a",
                role="worker",
                version="1",
                permissions={"read"},
                allowed_targets={"agent-b"},
                status=AgentStatus.ACTIVE,
            )
        )
        registry.register(
            AgentRecord(
                runtime_agent_id="runtime-b",
                logical_agent_id="agent-b",
                role="worker",
                version="1",
                permissions={"read"},
                allowed_targets=set(),
                status=AgentStatus.ACTIVE,
            )
        )

        pipeline = AnalysisPipeline(registry=registry)

        batch = [
            {
                "event_id": "replace-1",
                "source_runtime_id": "runtime-a",
                "target_runtime_id": "runtime-b",
                "timestamp": "2026-01-01T00:00:00Z",
            },
            {
                "event_id": "replace-2",
                "source_runtime_id": "runtime-a",
                "target_runtime_id": "runtime-b",
                "timestamp": "2026-01-01T00:00:01Z",
            },
        ]

        result = pipeline.process(batch, batch_id="replacement-batch")

        self.assertIn("replacement_runtime_ids", result)
        self.assertEqual(result["replacement_runtime_ids"], {})
        self.assertEqual(
            registry.get("runtime-a").status,
            AgentStatus.SUSPICIOUS,
        )


    def test_pipeline_policy_uses_cumulative_cross_batch_risk(self):
        from sentinelagent.models import AgentRecord, AgentStatus
        from sentinelagent.registry import AgentRegistry

        registry = AgentRegistry()
        registry.register(
            AgentRecord(
                logical_agent_id="agent-A",
                runtime_agent_id="runtime-A",
                role="worker",
                version="1.0",
                permissions={"message:send"},
                allowed_targets={"agent-B"},
            )
        )
        registry.register(
            AgentRecord(
                logical_agent_id="agent-B",
                runtime_agent_id="runtime-B",
                role="worker",
                version="1.0",
                permissions={"message:receive"},
                allowed_targets={"agent-A"},
            )
        )
        registry.activate("runtime-A")
        registry.activate("runtime-B")

        pipeline = AnalysisPipeline(registry=registry)
        first_batch = [
            {
                "event_id": "cross-batch-policy-1",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-B",
            }
        ]
        second_batch = [
            {
                "event_id": "cross-batch-policy-2",
                "source_runtime_id": "runtime-A",
                "target_runtime_id": "runtime-B",
            }
        ]

        first_result = pipeline.process(
            first_batch,
            batch_id="cross-batch-policy-batch-1",
        )
        self.assertEqual(
            first_result["policy_decisions_json"]["runtime-A"]["action"],
            "MONITOR",
        )
        self.assertEqual(
            registry.get("runtime-A").status,
            AgentStatus.ACTIVE,
        )

        second_result = pipeline.process(
            second_batch,
            batch_id="cross-batch-policy-batch-2",
        )

        self.assertEqual(
            second_result["cross_batch_risk_assessments"]["runtime-A"][
                "risk_level"
            ],
            "MEDIUM",
        )
        self.assertEqual(
            second_result["policy_decisions_json"]["runtime-A"]["action"],
            "MARK_SUSPICIOUS",
        )
        self.assertEqual(
            registry.get("runtime-A").status,
            AgentStatus.SUSPICIOUS,
        )

    def test_analysis_pipeline_routes_edge_events_through_stream(self):
        from sentinelagent.models import (
            AgentRecord,
            EdgeDecision,
            EdgeRequest,
        )
        from sentinelagent.registry import AgentRegistry

        registry = AgentRegistry()
        registry.register(
            AgentRecord(
                logical_agent_id="agent-A",
                runtime_agent_id="runtime-A-001",
                role="ingest",
                version="1.0",
                permissions={"message:send"},
                allowed_targets={"agent-B"},
            )
        )
        registry.register(
            AgentRecord(
                logical_agent_id="agent-B",
                runtime_agent_id="runtime-B-001",
                role="validator",
                version="1.0",
                permissions={"validation:execute"},
                allowed_targets={"agent-A"},
            )
        )
        registry.activate("runtime-A-001")
        registry.activate("runtime-B-001")

        pipeline = AnalysisPipeline(registry=registry, batch_size=1)
        request = EdgeRequest(
            request_id="request-pipeline-001",
            trace_id="trace-pipeline-001",
            edge_id="edge-agent-A-agent-B",
            source_runtime_id="runtime-A-001",
            target_runtime_id="runtime-B-001",
            action="SEND_MESSAGE",
            requested_permission="message:send",
        )

        edge_result = pipeline.handle_edge_request(request)
        self.assertEqual(edge_result.decision, EdgeDecision.ALLOW)
        self.assertEqual(pipeline.event_stream.size(), 1)

        analysis_result = pipeline.process_pending_events()

        self.assertIsNotNone(analysis_result)
        self.assertEqual(analysis_result["event_count"], 1)
        self.assertEqual(analysis_result["invalid_events"], 0)
        self.assertEqual(
            analysis_result["events_by_source_runtime"],
            {"runtime-A-001": 1},
        )
        self.assertEqual(
            analysis_result["runtime_target_interactions"],
            {"runtime-A-001": {"runtime-B-001": 1}},
        )
        self.assertEqual(pipeline.event_stream.size(), 0)

    def test_analysis_pipeline_routes_denied_edge_events_through_stream(self):
        from sentinelagent.models import (
            AgentRecord,
            AgentStatus,
            EdgeDecision,
            EdgeRequest,
        )
        from sentinelagent.registry import AgentRegistry

        registry = AgentRegistry()
        registry.register(
            AgentRecord(
                logical_agent_id="agent-A",
                runtime_agent_id="runtime-A-denied",
                role="ingest",
                version="1.0",
                permissions={"message:read"},
                allowed_targets={"agent-B"},
            )
        )
        registry.register(
            AgentRecord(
                logical_agent_id="agent-B",
                runtime_agent_id="runtime-B-denied",
                role="validator",
                version="1.0",
                permissions={"validation:execute"},
                allowed_targets={"agent-A"},
            )
        )
        registry.activate("runtime-A-denied")
        registry.activate("runtime-B-denied")

        pipeline = AnalysisPipeline(registry=registry, batch_size=1)
        request = EdgeRequest(
            request_id="request-pipeline-denied-001",
            trace_id="trace-pipeline-denied-001",
            edge_id="edge-agent-A-agent-B-denied",
            source_runtime_id="runtime-A-denied",
            target_runtime_id="runtime-B-denied",
            action="SEND_MESSAGE",
            requested_permission="message:send",
        )

        edge_result = pipeline.handle_edge_request(request)

        self.assertEqual(edge_result.decision, EdgeDecision.DENY)
        self.assertEqual(pipeline.event_stream.size(), 1)

        analysis_result = pipeline.process_pending_events()

        self.assertIsNotNone(analysis_result)
        self.assertEqual(analysis_result["event_count"], 1)
        self.assertEqual(analysis_result["invalid_events"], 0)
        self.assertEqual(
            analysis_result["events_by_source_runtime"],
            {"runtime-A-denied": 1},
        )
        self.assertEqual(
            analysis_result["runtime_target_interactions"],
            {"runtime-A-denied": {"runtime-B-denied": 1}},
        )
        self.assertEqual(
            registry.get("runtime-A-denied").status,
            AgentStatus.ACTIVE,
        )
        self.assertEqual(
            registry.get("runtime-B-denied").status,
            AgentStatus.ACTIVE,
        )
        self.assertEqual(pipeline.event_stream.size(), 0)


    def test_repeated_denied_unknown_runtime_does_not_break_batch_processing(self):
        from sentinelagent.models import AgentRecord, AgentStatus
        from sentinelagent.registry import AgentRegistry

        registry = AgentRegistry()
        registry.register(
            AgentRecord(
                logical_agent_id="agent-B",
                runtime_agent_id="runtime-B",
                role="validator",
                version="1.0",
                permissions={"message:receive"},
                allowed_targets=set(),
                status=AgentStatus.ACTIVE,
            )
        )
        pipeline = AnalysisPipeline(registry=registry)

        first_event = {
            "event_id": "unknown-denied-1",
            "decision": "DENY",
            "source_agent": {
                "logical_id": None,
                "runtime_id": "runtime-unknown",
                "status": None,
            },
            "target_agent": {
                "logical_id": "agent-B",
                "runtime_id": "runtime-B",
                "status": "ACTIVE",
            },
            "action": "SEND_MESSAGE",
            "requested_permission": "message:send",
        }
        second_event = {
            **first_event,
            "event_id": "unknown-denied-2",
        }

        first_result = pipeline.process(
            [first_event], batch_id="unknown-denied-batch-1"
        )
        second_result = pipeline.process(
            [second_event], batch_id="unknown-denied-batch-2"
        )

        self.assertEqual(
            second_result["policy_decisions_json"]["runtime-unknown"]["action"],
            "MARK_SUSPICIOUS",
        )
        self.assertEqual(
            second_result["enforced_actions"]["runtime-unknown"],
            "SKIPPED_UNKNOWN_RUNTIME",
        )
        self.assertEqual(registry.get("runtime-B").status, AgentStatus.ACTIVE)
        self.assertIn("runtime-unknown", first_result["policy_decisions_json"])


if __name__ == "__main__":
    unittest.main()
