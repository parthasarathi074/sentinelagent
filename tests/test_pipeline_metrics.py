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

    assert metrics == {
        "processed": 2,
        "failed": 0,
        "retried": 0,
        "quarantined": 0,
    }


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

    assert metrics == {
        "processed": 0,
        "failed": 1,
        "retried": 0,
        "quarantined": 0,
    }


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

    assert metrics == {
        "processed": 2,
        "failed": 0,
        "retried": 0,
        "quarantined": 0,
    }

    # Duplicate processing does not double-count accumulated evidence.
    assert pipeline.snapshot()["batches_processed"] == 1
