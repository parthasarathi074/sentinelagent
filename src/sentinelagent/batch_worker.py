import logging
from collections.abc import Callable
from time import perf_counter
from typing import Any


logger = logging.getLogger(__name__)


class BatchWorker:
    """
    Delivers completed event batches to an analysis handler.

    Logs batch outcomes without logging event contents.
    The worker does not perform security analysis itself.
    """

    def __init__(
        self,
        batch_handler: Callable[[list[Any]], Any],
    ) -> None:
        self.batch_handler = batch_handler

    def process(
        self,
        batch: list[Any],
        batch_id: str | None = None,
    ) -> Any:
        """
        Send one completed batch to the analysis handler.

        Args:
            batch: Events in the completed batch.
            batch_id: Stable identifier reused when retrying this batch.

        Raises:
            ValueError: If the batch is empty or batch_id is invalid.
            Exception: Re-raises any exception raised by the handler.
        """
        if not batch:
            raise ValueError("batch must not be empty")

        if batch_id is not None:
            if not isinstance(batch_id, str) or not batch_id.strip():
                raise ValueError("batch_id must be a non-empty string")

        event_count = len(batch)
        started_at = perf_counter()

        logger.info(
            "batch_processing_started batch_id=%s event_count=%d",
            batch_id or "unassigned",
            event_count,
        )

        try:
            if batch_id is not None:
                result = self.batch_handler(batch, batch_id=batch_id)
            else:
                result = self.batch_handler(batch)

        except Exception:
            duration_seconds = perf_counter() - started_at

            logger.exception(
                (
                    "batch_processing_finished batch_id=%s event_count=%d "
                    "status=failure duration_seconds=%.6f"
                ),
                batch_id or "unassigned",
                event_count,
                duration_seconds,
            )
            raise

        duration_seconds = perf_counter() - started_at

        logger.info(
            (
                "batch_processing_finished batch_id=%s event_count=%d "
                "status=success duration_seconds=%.6f"
            ),
            batch_id or "unassigned",
            event_count,
            duration_seconds,
        )

        return result
