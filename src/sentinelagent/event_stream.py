from collections import deque
import time
from collections.abc import Callable
from typing import Any


class EventStream:
    """
    Simple in-memory event stream used by SentinelAgent during development.

    Preserves event arrival timestamps and publication order when consumed
    batches are restored.
    """

    def __init__(self, clock: Callable[[], float] | None = None) -> None:
        self._events: deque[Any] = deque()
        self._arrival_times: deque[float] = deque()
        self._sequence_numbers: deque[int] = deque()
        self._clock = clock if clock is not None else time.monotonic
        self._next_sequence_number = 0
        self._next_front_sequence_number = -1
        self._consumed_batches: dict[
            int, tuple[list[Any], list[float], list[int]]
        ] = {}

    def set_clock(self, clock: Callable[[], float]) -> None:
        """Set the clock used to timestamp newly published events."""
        self._clock = clock

    def publish(self, event: Any) -> None:
        """Add an event with its arrival time and publication sequence."""
        self._events.append(event)
        self._arrival_times.append(self._clock())
        self._sequence_numbers.append(self._next_sequence_number)
        self._next_sequence_number += 1

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
        arrival_times: list[float] = []
        sequence_numbers: list[int] = []

        while self._events and len(batch) < max_events:
            batch.append(self._events.popleft())
            arrival_times.append(self._arrival_times.popleft())
            sequence_numbers.append(self._sequence_numbers.popleft())

        if batch:
            self._consumed_batches[id(batch)] = (
                batch,
                arrival_times,
                sequence_numbers,
            )

        return batch

    def restore_batch(self, batch: list[Any]) -> None:
        """Restore a batch while preserving its order and original timestamps."""
        if not batch:
            return

        saved_batch = self._consumed_batches.pop(id(batch), None)

        if saved_batch is not None and saved_batch[0] is batch:
            arrival_times = saved_batch[1]
            sequence_numbers = saved_batch[2]
        else:
            arrival_times = [self._clock()] * len(batch)
            first_sequence = (
                self._next_front_sequence_number - len(batch) + 1
            )
            sequence_numbers = list(
                range(first_sequence, self._next_front_sequence_number + 1)
            )
            self._next_front_sequence_number -= len(batch)

        restored_items = list(zip(batch, arrival_times, sequence_numbers))
        existing_items = list(
            zip(self._events, self._arrival_times, self._sequence_numbers)
        )
        combined = existing_items + restored_items
        combined.sort(key=lambda item: item[2])

        self._events = deque(event for event, _, _ in combined)
        self._arrival_times = deque(
            arrival_time for _, arrival_time, _ in combined
        )
        self._sequence_numbers = deque(
            sequence_number for _, _, sequence_number in combined
        )
