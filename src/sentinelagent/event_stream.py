from collections import deque
import time
from collections.abc import Callable
from typing import Any


class EventStream:
    """
    Simple in-memory event stream used by SentinelAgent during development.

    Tracks event arrival times separately from event payloads.
    """

    def __init__(self, clock: Callable[[], float] | None = None) -> None:
        self._events: deque[Any] = deque()
        self._arrival_times: deque[float] = deque()
        self._clock = clock if clock is not None else time.monotonic

    def set_clock(self, clock: Callable[[], float]) -> None:
        """Set the clock used to timestamp newly published events."""
        self._clock = clock

    def publish(self, event: Any) -> None:
        """Add an event and record its arrival time."""
        self._events.append(event)
        self._arrival_times.append(self._clock())

    def size(self) -> int:
        """Return the number of events currently waiting."""
        return len(self._events)

    def oldest_event_time(self) -> float | None:
        """Return the oldest waiting event's arrival time, if available."""
        if not self._arrival_times:
            return None
        return self._arrival_times[0]

    def consume_batch(self, max_events: int) -> list[Any]:
        """Remove and return up to max_events in publication order."""
        if max_events <= 0:
            raise ValueError("max_events must be greater than zero")

        batch: list[Any] = []

        while self._events and len(batch) < max_events:
            batch.append(self._events.popleft())
            self._arrival_times.popleft()

        return batch

    def restore_batch(self, batch: list[Any]) -> None:
        """Restore a batch to the front, preserving its order."""
        restored_at = self._clock()
        for event in reversed(batch):
            self._events.appendleft(event)
            self._arrival_times.appendleft(restored_at)
