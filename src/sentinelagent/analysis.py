from typing import Any


class AnalysisEngine:
    """
    Produces structured evidence from a completed batch of security events.

    The first version performs only basic batch analysis:
    - counts events
    - records event IDs

    More advanced behavioral, temporal, graph, and collusion
    analysis will be added later.
    """

    def analyze(self, batch: list[Any]) -> dict[str, Any]:
        """
        Analyze one completed event batch.

        Returns:
            A dictionary containing the event count and event IDs.
        """
        return {
            "event_count": len(batch),
            "event_ids": [
                event["event_id"]
                for event in batch
            ],
        }