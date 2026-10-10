import time
from collections.abc import Callable
from typing import Any


class MicroBatchProcessor:
    """
    Collects events from an EventStream and sends them to a batch handler.

    A batch is processed when either:
    - the configured batch size is reached, or
    - a partial batch has remained in the stream for at least
      max_wait_seconds, measured from the first time process_once()
      observes a non-empty stream.

    The clock can be injected for deterministic testing.
    """

    def __init__(
        self,
        event_stream: Any,
        batch_size: int,
        batch_handler: Callable[[list[Any]], None],
        max_wait_seconds: float = 1.0,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than zero")

        if max_wait_seconds <= 0:
            raise ValueError("max_wait_seconds must be greater than zero")

        self.event_stream = event_stream
        self.batch_size = batch_size
        self.batch_handler = batch_handler
        self.max_wait_seconds = max_wait_seconds
        self.clock = clock if clock is not None else time.monotonic

        self._batch_started_at: float | None = None

    def process_once(self) -> list[Any]:
        """
        Process one batch when either batching condition is satisfied.

        The timeout begins when this method first observes a non-empty
        stream. Returns an empty list when no processing condition is met.
        """
        current_size = self.event_stream.size()

        if current_size == 0:
            self._batch_started_at = None
            return []

        now = self.clock()

        if self._batch_started_at is None:
            self._batch_started_at = now

        batch_size_reached = current_size >= self.batch_size
        max_wait_reached = (
            now - self._batch_started_at
        ) >= self.max_wait_seconds

        if not batch_size_reached and not max_wait_reached:
            return []

        batch = self.event_stream.consume_batch(self.batch_size)

        if batch:
            try:
                self.batch_handler(batch)
            except Exception:
                self.event_stream.restore_batch(batch)
                self._batch_started_at = None
                raise

        if self.event_stream.size() == 0:
            self._batch_started_at = None
        else:
            self._batch_started_at = self.clock()

        return batch