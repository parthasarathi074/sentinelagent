import hashlib
import hashlib
import json
from typing import Any

from sentinelagent.interaction_graph import InteractionGraph


class CrossBatchAnalyzer:
    """Accumulate runtime interaction evidence across completed batches."""

    def __init__(self) -> None:
        self._interaction_counts: dict[str, dict[str, int]] = {}
        self._batches_processed = 0
        self._processed_batch_ids: set[str] = set()
        self._processed_batch_fingerprints: dict[str, str] = {}

    def _fingerprint(self, analysis_result: dict[str, Any]) -> str:
        """Create a stable fingerprint of a batch analysis result."""
        serialized = json.dumps(
            analysis_result,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def process_batch(
        self,
        analysis_result: dict[str, Any],
        batch_id: str | None = None,
    ) -> None:
        """Add interaction evidence from one batch analysis result."""
        if batch_id is not None:
            if not isinstance(batch_id, str) or not batch_id.strip():
                raise ValueError("batch_id must be a non-empty string")
            fingerprint = self._fingerprint(analysis_result)
            if batch_id in self._processed_batch_ids:
                previous_fingerprint = (
                    self._processed_batch_fingerprints[batch_id]
                )
                if fingerprint != previous_fingerprint:
                    raise ValueError(
                        f"batch_id '{batch_id}' was reused with different content"
                    )
                return

        interactions = analysis_result.get("runtime_target_interactions", {})
        pending_updates: list[tuple[str, str, int]] = []

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

                    pending_updates.append(
                        (source_runtime_id, target_runtime_id, count)
                    )

        staged_counts = type(self._interaction_counts)()
        staged_counts.update(
            (source_runtime_id, dict(targets))
            for source_runtime_id, targets in self._interaction_counts.items()
        )
        for source_runtime_id, target_runtime_id, count in pending_updates:
            source_counts = staged_counts.setdefault(source_runtime_id, {})
            source_counts[target_runtime_id] = (
                source_counts.get(target_runtime_id, 0) + count
            )

        self._interaction_counts = staged_counts
        self._batches_processed += 1
        if batch_id is not None:
            self._processed_batch_ids.add(batch_id)
            self._processed_batch_fingerprints[batch_id] = fingerprint

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

        interaction_graph = InteractionGraph(interaction_counts)

        return {
            "batches_processed": self._batches_processed,
            "runtime_target_interactions": interaction_counts,
            "repeated_runtime_interactions": repeated_interactions,
            "shared_target_interactions": shared_target_interactions,
            "interaction_graph": interaction_graph.snapshot(),
            "runtime_risk_assessments": runtime_risk_assessments,
        }
