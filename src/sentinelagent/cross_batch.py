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
        """Return a copy of the accumulated evidence."""
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

        return {
            "batches_processed": self._batches_processed,
            "runtime_target_interactions": interaction_counts,
            "repeated_runtime_interactions": repeated_interactions,
            "shared_target_interactions": shared_target_interactions,
        }
