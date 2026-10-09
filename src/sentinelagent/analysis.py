from datetime import datetime, timezone
from typing import Any

from sentinelagent.interaction_graph import InteractionGraph
from sentinelagent.collusion import CollusionAssessor


def _parse_timestamp(timestamp: str) -> datetime:
    """Parse an ISO 8601 timestamp and normalize it to UTC.

    Timestamps without timezone information are interpreted as UTC.
    Timestamps with timezone information are converted to UTC.
    Invalid timestamps raise ValueError for the caller to handle.
    """
    parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


class AnalysisEngine:
    """
    Produces structured evidence from a completed batch of security events.

    The analysis performs:
    - event counting
    - event ID collection
    - event counts by source runtime
    - interaction counts between source and target runtimes
    - repeated interaction detection
    - shared-target interaction detection
    - interaction graph construction
    - pairwise coordination evidence
    - group coordination evidence
    - runtime activity window tracking
    - runtime event-rate calculation
    - runtime risk assessment
    """

    def analyze(self, batch: list[Any]) -> dict[str, Any]:
        """
        Analyze one completed event batch.

        Returns:
            A dictionary containing event, interaction, graph,
            behavioral, temporal, and risk evidence.
        """
        events_by_source_runtime: dict[str, int] = {}
        runtime_target_interactions: dict[str, dict[str, int]] = {}
        runtime_activity_windows: dict[str, dict[str, str]] = {}
        event_ids: list[str] = []
        invalid_events = 0

        for event in batch:
            if not isinstance(event, dict):
                invalid_events += 1
                continue

            event_id = event.get("event_id")

            if not isinstance(event_id, str) or not event_id.strip():
                invalid_events += 1
            else:
                event_ids.append(event_id)

            source_runtime_id = event.get("source_runtime_id")

            if (
                not isinstance(source_runtime_id, str)
                or not source_runtime_id.strip()
            ):
                invalid_events += 1
                continue

            events_by_source_runtime[source_runtime_id] = (
                events_by_source_runtime.get(source_runtime_id, 0) + 1
            )

            target_runtime_id = event.get("target_runtime_id")

            if target_runtime_id is not None:
                if (
                    not isinstance(target_runtime_id, str)
                    or not target_runtime_id.strip()
                ):
                    invalid_events += 1
                else:
                    interactions = runtime_target_interactions.setdefault(
                        source_runtime_id, {}
                    )
                    interactions[target_runtime_id] = (
                        interactions.get(target_runtime_id, 0) + 1
                    )

            timestamp = event.get("timestamp")

            if timestamp is None:
                continue

            try:
                parsed_timestamp = _parse_timestamp(timestamp)
            except (ValueError, TypeError, AttributeError):
                continue

            normalized_timestamp = parsed_timestamp.isoformat()

            if source_runtime_id not in runtime_activity_windows:
                runtime_activity_windows[source_runtime_id] = {
                    "first_event_timestamp": normalized_timestamp,
                    "last_event_timestamp": normalized_timestamp,
                }
            else:
                current_window = runtime_activity_windows[source_runtime_id]
                first_timestamp = _parse_timestamp(
                    current_window["first_event_timestamp"]
                )
                last_timestamp = _parse_timestamp(
                    current_window["last_event_timestamp"]
                )

                if parsed_timestamp < first_timestamp:
                    current_window["first_event_timestamp"] = (
                        normalized_timestamp
                    )

                if parsed_timestamp > last_timestamp:
                    current_window["last_event_timestamp"] = (
                        normalized_timestamp
                    )

        runtime_event_rates: dict[str, float] = {}

        for source_runtime_id, count in events_by_source_runtime.items():
            activity_window = runtime_activity_windows.get(source_runtime_id)

            if activity_window is None:
                runtime_event_rates[source_runtime_id] = 0.0
                continue

            first_timestamp = _parse_timestamp(
                activity_window["first_event_timestamp"]
            )
            last_timestamp = _parse_timestamp(
                activity_window["last_event_timestamp"]
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

        target_source_interactions: dict[str, dict[str, int]] = {}

        for source_runtime_id, interactions in runtime_target_interactions.items():
            for target_runtime_id, count in interactions.items():
                if count <= 1:
                    continue

                target_source_interactions.setdefault(target_runtime_id, {})[
                    source_runtime_id
                ] = count

        shared_target_interactions = {
            target_runtime_id: source_interactions
            for target_runtime_id, source_interactions in target_source_interactions.items()
            if len(source_interactions) > 1
        }

        interaction_graph = InteractionGraph(runtime_target_interactions)

        runtime_risk_assessments: dict[str, dict[str, Any]] = {}

        all_runtime_ids = set(events_by_source_runtime)

        for source_runtime_id in all_runtime_ids:
            risk_score = 0
            reasons: list[str] = []

            repeated_targets = repeated_runtime_interactions.get(
                source_runtime_id, {}
            )

            if repeated_targets:
                risk_score += 20
                reasons.append("REPEATED_TARGET_INTERACTIONS")

            shared_targets = [
                target_runtime_id
                for target_runtime_id, source_interactions
                in shared_target_interactions.items()
                if source_runtime_id in source_interactions
            ]

            if shared_targets:
                risk_score += 30
                reasons.append("SHARED_TARGET_INTERACTIONS")

            if risk_score >= 50:
                risk_level = "HIGH"
            elif risk_score >= 20:
                risk_level = "MEDIUM"
            else:
                risk_level = "LOW"

            runtime_risk_assessments[source_runtime_id] = {
                "risk_score": risk_score,
                "risk_level": risk_level,
                "reasons": reasons,
            }

        return {
            "event_count": len(batch),
            "event_ids": event_ids,
            "invalid_events": invalid_events,
            "events_by_source_runtime": events_by_source_runtime,
            "runtime_target_interactions": runtime_target_interactions,
            "repeated_runtime_interactions": repeated_runtime_interactions,
            "shared_target_interactions": shared_target_interactions,
            "interaction_graph": interaction_graph.snapshot(),
            "collusion_assessment": CollusionAssessor().assess(runtime_target_interactions),
            "runtime_activity_windows": runtime_activity_windows,
            "runtime_event_rates": runtime_event_rates,
            "runtime_risk_assessments": runtime_risk_assessments,
        }


if __name__ == "__main__":
    pass
