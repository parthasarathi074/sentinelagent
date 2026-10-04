from __future__ import annotations

from typing import Any


class InteractionGraph:
    """Build a deterministic interaction graph from runtime interaction counts."""

    def __init__(self, interactions: dict[str, dict[str, int]] | None = None) -> None:
        self._interactions: dict[str, dict[str, int]] = {}
        if interactions is not None:
            self.update(interactions)

    def update(self, interactions: dict[str, dict[str, int]]) -> None:
        """Replace graph evidence with validated interaction counts."""
        if not isinstance(interactions, dict):
            raise ValueError("interactions must be a dictionary")

        normalized: dict[str, dict[str, int]] = {}

        for source_runtime_id, targets in interactions.items():
            if (
                not isinstance(source_runtime_id, str)
                or not source_runtime_id.strip()
                or not isinstance(targets, dict)
            ):
                continue

            for target_runtime_id, count in targets.items():
                if (
                    not isinstance(target_runtime_id, str)
                    or not target_runtime_id.strip()
                    or isinstance(count, bool)
                    or not isinstance(count, int)
                    or count < 0
                ):
                    continue

                normalized.setdefault(source_runtime_id, {})[
                    target_runtime_id
                ] = count

        self._interactions = normalized

    def nodes(self) -> list[str]:
        """Return all observed runtime IDs in deterministic order."""
        node_ids: set[str] = set(self._interactions)

        for targets in self._interactions.values():
            node_ids.update(targets)

        return sorted(node_ids)

    def edges(self) -> list[dict[str, Any]]:
        """Return directed interaction edges in deterministic order."""
        edges: list[dict[str, Any]] = []

        for source_runtime_id in sorted(self._interactions):
            for target_runtime_id in sorted(
                self._interactions[source_runtime_id]
            ):
                count = self._interactions[source_runtime_id][target_runtime_id]
                edges.append(
                    {
                        "source_runtime_id": source_runtime_id,
                        "target_runtime_id": target_runtime_id,
                        "interaction_count": count,
                        "repeated": count > 1,
                    }
                )

        return edges

    def pairwise_coordination(self) -> list[dict[str, Any]]:
        """Return repeated directed interactions as pairwise evidence."""
        return [
            {
                "source_runtime_id": edge["source_runtime_id"],
                "target_runtime_id": edge["target_runtime_id"],
                "interaction_count": edge["interaction_count"],
                "signals": ["REPEATED_INTERACTION"],
            }
            for edge in self.edges()
            if edge["repeated"]
        ]

    def group_coordination(self) -> list[dict[str, Any]]:
        """
        Return groups of runtimes that repeatedly interact with a shared target.

        The shared target is retained as evidence context rather than being
        treated as a member of the coordinating source group.
        """
        target_sources: dict[str, dict[str, int]] = {}

        for edge in self.edges():
            if not edge["repeated"]:
                continue

            target_sources.setdefault(
                edge["target_runtime_id"], {}
            )[edge["source_runtime_id"]] = edge["interaction_count"]

        groups: list[dict[str, Any]] = []

        for target_runtime_id in sorted(target_sources):
            source_interactions = target_sources[target_runtime_id]

            if len(source_interactions) < 2:
                continue

            source_runtime_ids = sorted(source_interactions)

            groups.append(
                {
                    "member_runtime_ids": source_runtime_ids,
                    "shared_target_runtime_id": target_runtime_id,
                    "interaction_counts": {
                        runtime_id: source_interactions[runtime_id]
                        for runtime_id in source_runtime_ids
                    },
                    "signals": ["SHARED_REPEATED_TARGET"],
                }
            )

        return groups

    def snapshot(self) -> dict[str, Any]:
        """Return the complete graph evidence."""
        return {
            "nodes": self.nodes(),
            "edges": self.edges(),
            "pairwise_coordination": self.pairwise_coordination(),
            "group_coordination": self.group_coordination(),
        }
