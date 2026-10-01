from collections.abc import Callable
from typing import Any


class BatchWorker:
    """
    Delivers completed event batches to an analysis handler.

    The worker is intentionally lightweight. It does not perform
    security analysis itself; it only validates and forwards batches.
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
        """
        if not batch:
            raise ValueError("batch must not be empty")

        if batch_id is not None:
            if not isinstance(batch_id, str) or not batch_id.strip():
                raise ValueError("batch_id must be a non-empty string")
            return self.batch_handler(batch, batch_id=batch_id)

        return self.batch_handler(batch)