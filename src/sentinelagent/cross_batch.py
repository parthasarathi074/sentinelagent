from typing import Any


class CrossBatchAnalyzer:
    """Accumulate runtime interaction evidence across completed batches."""

    def __init__(self) -> None:
        self._interaction_counts: dict[str, dict[str, int]] = {}
        self._batches_processed = 0

    def process_batch(self, analysis_result: dict[str, Any]) -> None:
        """Add interaction evidence from one batch analysis result."""
        interactions = analysis_result.get("runtime_target_interactions", {})

        if isinstance(interactions, dict):
            for source_runtime_id, targets in interactions.items():
                if (
                    not isinstance(source_runtime_id, str)
                    or not source_runtime_id.strip()
                ):
                    continue

                if not isinstance(targets, dict):
                    continue

                for target_runtime_id, count in targets.items():
                    if (
                        not isinstance(target_runtime_id, str)
                        or not target_runtime_id.strip()
                    ):
                        continue

                    if (
                        isinstance(count, bool)
                        or not isinstance(count, int)
                        or count < 0
                    ):
                        continue

                    source_counts = self._interaction_counts.setdefault(
                        source_runtime_id, {}
                    )
                    source_counts[target_runtime_id] = (
                        source_counts.get(target_runtime_id, 0) + count
                    )

        self._batches_processed += 1

    def snapshot(self) -> dict[str, Any]:
        """Return accumulated interaction evidence and risk assessments."""
        interaction_counts = {
            source: dict(targets)
            for source, targets in self._interaction_counts.items()
        }
        repeated_interactions = {
            source: {
                target: count
                for target, count in targets.items()
                if count > 1
            }
            for source, targets in interaction_counts.items()
        }
        repeated_interactions = {
            source: targets
            for source, targets in repeated_interactions.items()
            if targets
        }
        target_sources: dict[str, dict[str, int]] = {}
        for source_runtime_id, targets in interaction_counts.items():
            for target_runtime_id, count in targets.items():
                if count > 0:
                    sources = target_sources.setdefault(target_runtime_id, {})
                    sources[source_runtime_id] = count

        shared_target_interactions = {
            target_runtime_id: source_interactions
            for target_runtime_id, source_interactions in target_sources.items()
            if len(source_interactions) > 1
        }

        runtime_risk_assessments: dict[str, dict[str, Any]] = {}
        for source_runtime_id in interaction_counts:
            risk_score = 0
            reasons: list[str] = []

            if repeated_interactions.get(source_runtime_id):
                risk_score += 20
                reasons.append("REPEATED_TARGET_INTERACTIONS")

            has_shared_target = any(
                source_runtime_id in source_interactions
                for source_interactions in shared_target_interactions.values()
            )
            if has_shared_target:
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
            "batches_processed": self._batches_processed,
            "runtime_target_interactions": interaction_counts,
            "repeated_runtime_interactions": repeated_interactions,
            "shared_target_interactions": shared_target_interactions,
            "runtime_risk_assessments": runtime_risk_assessments,
        }
