import time
import uuid
from collections.abc import Callable
from typing import Any


class MicroBatchProcessor:
    """
    Collect events from an EventStream and sends them to a batch handler.

    A batch is processed when either:
    - the configured batch size is reached, or
    - the oldest waiting event has waited for max_wait_seconds.

    Streams may optionally support acknowledge_batch() to finalize
    successfully processed batches while retaining audit history.
    An ID-aware handler takes precedence over the legacy batch handler.
    Streams can provide batch_id_for(batch) for IDs that persist with a
    restored batch; generated IDs are retained for retries in this processor.
    """

    def __init__(
        self,
        event_stream: Any,
        batch_size: int,
        batch_handler: Callable[[list[Any]], None],
        max_wait_seconds: float = 1.0,
        clock: Callable[[], float] | None = None,
        on_retry: Callable[[int], None] | None = None,
        batch_handler_with_id: Callable[[list[Any], str], None] | None = None,
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
        self.on_retry = on_retry
        self.batch_handler_with_id = batch_handler_with_id
        self._retry_events_pending = 0
        self._retry_batch_id: str | None = None

        set_stream_clock = getattr(event_stream, "set_clock", None)
        if callable(set_stream_clock):
            set_stream_clock(self.clock)

        self._batch_started_at: float | None = None

    def process_once(self) -> list[Any]:
        """Process one batch when either batching condition is satisfied."""
        current_size = self.event_stream.size()

        if current_size == 0:
            self._batch_started_at = None
            return []

        now = self.clock()
        oldest_event_time = getattr(
            self.event_stream, "oldest_event_time", None
        )

        batch_started_at = (
            oldest_event_time() if callable(oldest_event_time) else None
        )

        if batch_started_at is None:
            if self._batch_started_at is None:
                self._batch_started_at = now
            batch_started_at = self._batch_started_at

        batch_size_reached = current_size >= self.batch_size
        max_wait_reached = (
            now - batch_started_at
        ) >= self.max_wait_seconds

        if not batch_size_reached and not max_wait_reached:
            return []

        batch = self.event_stream.consume_batch(self.batch_size)

        if batch:
            retry_event_count = min(
                len(batch), self._retry_events_pending
            )
            self._retry_events_pending -= retry_event_count

            try:
                if retry_event_count and self.on_retry is not None:
                    self.on_retry(retry_event_count)
                if self.batch_handler_with_id is None:
                    self.batch_handler(batch)
                else:
                    batch_id_for = getattr(
                        self.event_stream,
                        "batch_id_for",
                        None,
                    )
                    batch_id = (
                        batch_id_for(batch)
                        if callable(batch_id_for)
                        else None
                    )
                    if batch_id is None:
                        batch_id = (
                            self._retry_batch_id
                            if self._retry_batch_id is not None
                            else str(uuid.uuid4())
                        )
                    if (
                        not isinstance(batch_id, str)
                        or not batch_id.strip()
                    ):
                        raise ValueError(
                            "batch_id_for must return a non-empty string"
                        )

                    self._retry_batch_id = batch_id
                    self.batch_handler_with_id(batch, batch_id)
                    self._retry_batch_id = None
            except Exception:
                self.event_stream.restore_batch(batch)
                self._retry_events_pending += len(batch)
                self._batch_started_at = None
                raise

            acknowledge_batch = getattr(
                self.event_stream, "acknowledge_batch", None
            )
            if callable(acknowledge_batch):
                acknowledge_batch(batch)

        if self.event_stream.size() == 0:
            self._batch_started_at = None
        else:
            self._batch_started_at = self.clock()

        return batch
