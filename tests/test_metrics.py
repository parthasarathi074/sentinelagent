from concurrent.futures import ThreadPoolExecutor

import pytest

from sentinelagent.metrics import Metrics


def test_metrics_start_at_zero() -> None:
    metrics = Metrics()

    assert metrics.snapshot().to_dict() == {
        "processed": 0,
        "failed": 0,
        "retried": 0,
        "quarantined": 0,
    }


def test_metrics_record_all_counter_types() -> None:
    metrics = Metrics()

    metrics.record_processed()
    metrics.record_failed(2)
    metrics.record_retried(3)
    metrics.record_quarantined(4)

    assert metrics.snapshot().to_dict() == {
        "processed": 1,
        "failed": 2,
        "retried": 3,
        "quarantined": 4,
    }


def test_metrics_snapshot_is_independent() -> None:
    metrics = Metrics()
    metrics.record_processed(2)

    earlier_snapshot = metrics.snapshot()
    metrics.record_processed(3)

    assert earlier_snapshot.processed == 2
    assert metrics.snapshot().processed == 5


def test_metrics_reset_clears_all_counters() -> None:
    metrics = Metrics()
    metrics.record_processed(5)
    metrics.record_failed(2)
    metrics.record_retried(3)
    metrics.record_quarantined(1)

    metrics.reset()

    assert metrics.snapshot().to_dict() == {
        "processed": 0,
        "failed": 0,
        "retried": 0,
        "quarantined": 0,
    }


@pytest.mark.parametrize(
    "count",
    [-1, 1.5, "2", None, True],
)
def test_metrics_reject_invalid_increment(count: object) -> None:
    metrics = Metrics()

    with pytest.raises(ValueError, match="non-negative integer"):
        metrics.record_processed(count)  # type: ignore[arg-type]

    assert metrics.snapshot().processed == 0


def test_metrics_allow_zero_increment() -> None:
    metrics = Metrics()

    metrics.record_processed(0)

    assert metrics.snapshot().processed == 0


def test_metrics_support_concurrent_increments() -> None:
    metrics = Metrics()
    worker_count = 8
    increments_per_worker = 1000

    def increment_many_times() -> None:
        for _ in range(increments_per_worker):
            metrics.record_processed()

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(increment_many_times)
            for _ in range(worker_count)
        ]
        for future in futures:
            future.result()

    assert metrics.snapshot().processed == (
        worker_count * increments_per_worker
    )
