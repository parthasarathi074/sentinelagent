from typing import Any


class AnalysisEngine:
    """
    Produces structured evidence from a completed batch of security events.

    The first version performs basic batch analysis:
    - counts events
    - records event IDs
    - counts events by source runtime
    - tracks runtime activity windows

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
            - first and last event timestamps for each runtime
        """
        events_by_source_runtime: dict[str, int] = {}
        runtime_activity_windows: dict[str, dict[str, str]] = {}

        for event in batch:
            source_runtime_id = event.get("source_runtime_id")

            if source_runtime_id is None:
                continue

            events_by_source_runtime[source_runtime_id] = (
                events_by_source_runtime.get(source_runtime_id, 0) + 1
            )

            timestamp = event.get("timestamp")

            if timestamp is None:
                continue

            if source_runtime_id not in runtime_activity_windows:
                runtime_activity_windows[source_runtime_id] = {
                    "first_event_timestamp": timestamp,
                    "last_event_timestamp": timestamp,
                }
            else:
                current_window = runtime_activity_windows[source_runtime_id]

                if timestamp < current_window["first_event_timestamp"]:
                    current_window["first_event_timestamp"] = timestamp

                if timestamp > current_window["last_event_timestamp"]:
                    current_window["last_event_timestamp"] = timestamp

        return {
            "event_count": len(batch),
            "event_ids": [
                event["event_id"]
                for event in batch
            ],
            "events_by_source_runtime": events_by_source_runtime,
            "runtime_activity_windows": runtime_activity_windows,
        }