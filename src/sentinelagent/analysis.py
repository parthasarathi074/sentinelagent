from datetime import datetime
from typing import Any


class AnalysisEngine:
    """
    Produces structured evidence from a completed batch of security events.

    The first version performs basic batch analysis:
    - counts events
    - records event IDs
    - counts events by source runtime
    - counts interactions between source and target runtimes
    - identifies repeated interactions between source and target runtimes
    - tracks runtime activity windows
    - calculates runtime event rates

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
            - interaction counts grouped by source and target runtime
            - repeated interactions grouped by source and target runtime
            - first and last event timestamps for each runtime
            - observed event rate for each runtime
        """
        events_by_source_runtime: dict[str, int] = {}
        runtime_target_interactions: dict[str, dict[str, int]] = {}
        runtime_activity_windows: dict[str, dict[str, str]] = {}

        for event in batch:
            source_runtime_id = event.get("source_runtime_id")

            if source_runtime_id is None:
                continue

            events_by_source_runtime[source_runtime_id] = (
                events_by_source_runtime.get(source_runtime_id, 0) + 1
            )

            target_runtime_id = event.get("target_runtime_id")

            if target_runtime_id is not None:
                interactions = runtime_target_interactions.setdefault(
                    source_runtime_id, {}
                )
                interactions[target_runtime_id] = (
                    interactions.get(target_runtime_id, 0) + 1
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

        runtime_event_rates: dict[str, float] = {}

        for source_runtime_id, count in events_by_source_runtime.items():
            activity_window = runtime_activity_windows.get(source_runtime_id)

            if activity_window is None:
                runtime_event_rates[source_runtime_id] = 0.0
                continue

            first_timestamp = datetime.fromisoformat(
                activity_window["first_event_timestamp"].replace("Z", "+00:00")
            )
            last_timestamp = datetime.fromisoformat(
                activity_window["last_event_timestamp"].replace("Z", "+00:00")
            )

            duration_seconds = (
                last_timestamp - first_timestamp
            ).total_seconds()

            if duration_seconds <= 0:
                runtime_event_rates[source_runtime_id] = 0.0
            else:
                runtime_event_rates[source_runtime_id] = (
                    count / duration_seconds
                )

        repeated_runtime_interactions: dict[str, dict[str, int]] = {}

        for source_runtime_id, interactions in runtime_target_interactions.items():
            repeated_interactions = {
                target_runtime_id: count
                for target_runtime_id, count in interactions.items()
                if count > 1
            }

            if repeated_interactions:
                repeated_runtime_interactions[source_runtime_id] = (
                    repeated_interactions
                )

        return {
            "event_count": len(batch),
            "event_ids": [
                event["event_id"]
                for event in batch
            ],
            "events_by_source_runtime": events_by_source_runtime,
            "runtime_target_interactions": runtime_target_interactions,
            "repeated_runtime_interactions": repeated_runtime_interactions,
            "runtime_activity_windows": runtime_activity_windows,
            "runtime_event_rates": runtime_event_rates,
        }