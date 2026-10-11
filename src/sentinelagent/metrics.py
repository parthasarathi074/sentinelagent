from __future__ import annotations

from dataclasses import dataclass, field
from threading import Lock


@dataclass
class MetricsSnapshot:
    """Immutable-style snapshot of SentinelAgent counters."""

    processed: int = 0
    failed: int = 0
    retried: int = 0
    quarantined: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "processed": self.processed,
            "failed": self.failed,
            "retried": self.retried,
            "quarantined": self.quarantined,
        }


@dataclass
class Metrics:
    """Thread-safe counters for SentinelAgent observability."""

    _processed: int = field(default=0, init=False, repr=False)
    _failed: int = field(default=0, init=False, repr=False)
    _retried: int = field(default=0, init=False, repr=False)
    _quarantined: int = field(default=0, init=False, repr=False)
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)

    def record_processed(self, count: int = 1) -> None:
        self._increment("_processed", count)

    def record_failed(self, count: int = 1) -> None:
        self._increment("_failed", count)

    def record_retried(self, count: int = 1) -> None:
        self._increment("_retried", count)

    def record_quarantined(self, count: int = 1) -> None:
        self._increment("_quarantined", count)

    def snapshot(self) -> MetricsSnapshot:
        with self._lock:
            return MetricsSnapshot(
                processed=self._processed,
                failed=self._failed,
                retried=self._retried,
                quarantined=self._quarantined,
            )

    def reset(self) -> None:
        with self._lock:
            self._processed = 0
            self._failed = 0
            self._retried = 0
            self._quarantined = 0

    def _increment(self, counter_name: str, count: int) -> None:
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ValueError("count must be a non-negative integer")

        with self._lock:
            setattr(self, counter_name, getattr(self, counter_name) + count)
