from collections import deque
from typing import Any


class EventStream:
    """
    Simple in-memory event stream used by SentinelAgent during development.

    This simulates the basic behavior we need from an event broker:
    - publish events
    - inspect stream size
    - consume events in batches

    Kafka will replace this component later.
    """

    def __init__(self) -> None:
        self._events: deque[Any] = deque()

    def publish(self, event: Any) -> None:
        """Add one event to the stream."""
        self._events.append(event)

    def size(self) -> int:
        """Return the number of events currently waiting."""
        return len(self._events)

    def consume_batch(self, max_events: int) -> list[Any]:
        """
        Remove and return up to max_events from the stream.

        Events are returned in the same order in which they were published.
        """
        if max_events <= 0:
            raise ValueError("max_events must be greater than zero")

        batch: list[Any] = []

        while self._events and len(batch) < max_events:
            batch.append(self._events.popleft())

        return batch

    def restore_batch(self, batch: list[Any]) -> None:
        """Restore a consumed batch to the front, preserving its order."""
        for event in reversed(batch):
            self._events.appendleft(event)