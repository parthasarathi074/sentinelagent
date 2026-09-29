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

    def process(self, batch: list[Any]) -> Any:
        """
        Send one completed batch to the analysis handler.

        Raises:
            ValueError: If the batch is empty.
        """
        if not batch:
            raise ValueError("batch must not be empty")

        return self.batch_handler(batch)