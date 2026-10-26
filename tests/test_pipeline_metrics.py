import pytest

from sentinelagent.analysis_pipeline import AnalysisPipeline


def test_pipeline_metrics_count_successfully_processed_events() -> None:
    pipeline = AnalysisPipeline()

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
    ]

    pipeline.process(batch, batch_id="metrics-batch-1")

    metrics = pipeline.metrics.snapshot().to_dict()

    assert {
        key: value
        for key, value in metrics.items()
        if key != "last_batch_duration_seconds"
    } == {
        "processed": 2,
        "failed": 0,
        "retried": 0,
        "quarantined": 0,
    }
    assert metrics["last_batch_duration_seconds"] is not None
    assert metrics["last_batch_duration_seconds"] >= 0


def test_pipeline_metrics_count_failed_processing_attempt() -> None:
    pipeline = AnalysisPipeline()

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

    try:
        pipeline.process(batch, batch_id="metrics-failed-batch")
    except RuntimeError as error:
        assert str(error) == "simulated analysis failure"
    else:
        raise AssertionError("Expected analysis to fail")

    metrics = pipeline.metrics.snapshot().to_dict()

    assert {
        key: value
        for key, value in metrics.items()
        if key != "last_batch_duration_seconds"
    } == {
        "processed": 0,
        "failed": 1,
        "retried": 0,
        "quarantined": 0,
    }
    assert metrics["last_batch_duration_seconds"] is not None
    assert metrics["last_batch_duration_seconds"] >= 0


def test_pipeline_metrics_count_duplicate_successful_calls() -> None:
    pipeline = AnalysisPipeline()

    batch = [
        {
            "event_id": "event-duplicate",
            "source_runtime_id": "runtime-A",
            "target_runtime_id": "runtime-B",
        }
    ]

    pipeline.process(batch, batch_id="metrics-duplicate-batch")
    pipeline.process(batch, batch_id="metrics-duplicate-batch")

    metrics = pipeline.metrics.snapshot().to_dict()

    assert {
        key: value
        for key, value in metrics.items()
        if key != "last_batch_duration_seconds"
    } == {
        "processed": 2,
        "failed": 0,
        "retried": 0,
        "quarantined": 0,
    }
    assert metrics["last_batch_duration_seconds"] is not None
    assert metrics["last_batch_duration_seconds"] >= 0

    # Duplicate processing does not double-count accumulated evidence.
    assert pipeline.snapshot()["batches_processed"] == 1


def test_pipeline_metrics_count_events_when_micro_batch_is_retried() -> None:
    pipeline = AnalysisPipeline(batch_size=2)
    original_analyze = pipeline.analysis_engine.analyze
    original_process = pipeline.process
    original_cross_batch_process = (
        pipeline.cross_batch_analyzer.process_batch
    )
    attempts = 0
    received_batch_ids = []
    analyzed_batch_ids = []

    def fail_once(batch):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("simulated analysis failure")
        return original_analyze(batch)

    def capture_process(batch, batch_id=None):
        received_batch_ids.append(batch_id)
        return original_process(batch, batch_id=batch_id)

    def capture_cross_batch_process(result, batch_id=None):
        analyzed_batch_ids.append(batch_id)
        return original_cross_batch_process(result, batch_id=batch_id)

    pipeline.analysis_engine.analyze = fail_once
    pipeline.process = capture_process
    pipeline.cross_batch_analyzer.process_batch = (
        capture_cross_batch_process
    )
    batch = [
        {
            "event_id": "retry-event-1",
            "source_runtime_id": "runtime-A",
            "target_runtime_id": "runtime-B",
        },
        {
            "event_id": "retry-event-2",
            "source_runtime_id": "runtime-A",
            "target_runtime_id": "runtime-B",
        },
    ]
    for event in batch:
        pipeline.event_stream.publish(event)

    with pytest.raises(RuntimeError, match="simulated analysis failure"):
        pipeline.process_pending_events()

    failed_metrics = pipeline.metrics.snapshot().to_dict()
    assert {
        key: value
        for key, value in failed_metrics.items()
        if key != "last_batch_duration_seconds"
    } == {
        "processed": 0,
        "failed": 1,
        "retried": 0,
        "quarantined": 0,
    }
    assert failed_metrics["last_batch_duration_seconds"] is not None
    assert failed_metrics["last_batch_duration_seconds"] >= 0

    result = pipeline.process_pending_events()

    assert result is not None
    assert result["event_count"] == 2
    assert len(received_batch_ids) == 2
    assert received_batch_ids[0]
    assert received_batch_ids[0] == received_batch_ids[1]
    assert analyzed_batch_ids == [received_batch_ids[1]]
    retried_metrics = pipeline.metrics.snapshot().to_dict()
    assert {
        key: value
        for key, value in retried_metrics.items()
        if key != "last_batch_duration_seconds"
    } == {
        "processed": 2,
        "failed": 1,
        "retried": 2,
        "quarantined": 0,
    }
    assert retried_metrics["last_batch_duration_seconds"] is not None
    assert retried_metrics["last_batch_duration_seconds"] >= 0
