from typing import Any


class AnalysisEngine:
    """
    Produces structured evidence from a completed batch of security events.

    The first version performs basic batch analysis:
    - counts events
    - records event IDs
    - counts events by source runtime

    More advanced behavioral, temporal, graph, and collusion
    analysis will be added later.
    """

    def analyze(self, batch: list[Any]) -> dict[str, Any]:
        """
        Analyze one completed event batch.

        Returns:
            A dictionary containing:
            - the total event count
            - event IDs
            - event counts grouped by source runtime
        """
        events_by_source_runtime: dict[str, int] = {}

        for event in batch:
            source_runtime_id = event.get("source_runtime_id")

            if source_runtime_id is not None:
                events_by_source_runtime[source_runtime_id] = (
                    events_by_source_runtime.get(source_runtime_id, 0) + 1
                )

        return {
            "event_count": len(batch),
            "event_ids": [
                event["event_id"]
                for event in batch
            ],
            "events_by_source_runtime": events_by_source_runtime,
        }
