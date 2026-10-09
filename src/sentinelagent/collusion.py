from __future__ import annotations

from typing import Any

NORMAL = "NORMAL"
SUSPICIOUS = "SUSPICIOUS"
HIGH_RISK = "HIGH_RISK"
COLLUSION = "COLLUSION"


class CollusionAssessor:
    """Explainable, observational assessment of possible coordinated behavior.

    Repeated communication is weak evidence, not proof. The strongest label here
    requires both a repeated shared-target group and reciprocal repeated traffic
    among at least two members. Labels never trigger enforcement themselves.
    """

    def assess(self, interactions: Any) -> dict[str, Any]:
        """Assess a runtime->target->count mapping or an interaction-graph snapshot."""
        if isinstance(interactions, dict) and "edges" in interactions:
            interactions = self._from_edges(interactions.get("edges"))
        if not isinstance(interactions, dict):
            interactions = {}

        normalized: dict[str, dict[str, int]] = {}
        for source, targets in interactions.items():
            if not isinstance(source, str) or not source.strip() or not isinstance(targets, dict):
                continue
            for target, count in targets.items():
                if (isinstance(target, str) and target.strip() and
                    isinstance(count, int) and not isinstance(count, bool) and count >= 0):
                    normalized.setdefault(source, {})[target] = count

        repeated = [
            {"source_runtime_id": s, "target_runtime_id": t, "interaction_count": n}
            for s, targets in sorted(normalized.items())
            for t, n in sorted(targets.items()) if n > 1
        ]
        target_sources: dict[str, dict[str, int]] = {}
        for item in repeated:
            target_sources.setdefault(item["target_runtime_id"], {})[item["source_runtime_id"]] = item["interaction_count"]
        groups = [
            {"member_runtime_ids": sorted(sources), "shared_target_runtime_id": target,
             "interaction_counts": {s: sources[s] for s in sorted(sources)}}
            for target, sources in sorted(target_sources.items()) if len(sources) >= 2
        ]
        repeated_pairs = {(x["source_runtime_id"], x["target_runtime_id"]) for x in repeated}
        reciprocal_pairs = [
            {"runtime_ids": [a, b], "signals": ["RECIPROCAL_REPEATED_INTERACTION"]}
            for a, b in sorted(repeated_pairs)
            if a < b and (b, a) in repeated_pairs
        ]
        group_members = {m for group in groups for m in group["member_runtime_ids"]}
        reciprocal_members = {m for pair in reciprocal_pairs for m in pair["runtime_ids"]}
        corroborated = bool(groups and reciprocal_pairs and group_members & reciprocal_members)

        reasons: list[str] = []
        if repeated:
            reasons.append("REPEATED_INTERACTIONS_OBSERVED_NOT_PROOF_OF_COLLUSION")
        if groups:
            reasons.append("MULTIPLE_RUNTIMES_REPEATEDLY_CONTACT_SHARED_TARGET")
        if reciprocal_pairs:
            reasons.append("RECIPROCAL_REPEATED_INTERACTIONS_OBSERVED")
        if corroborated:
            reasons.append("SHARED_TARGET_AND_RECIPROCAL_INTERACTION_SIGNALS_COMBINED")

        if corroborated:
            state = COLLUSION
        elif groups:
            state = HIGH_RISK
        elif repeated:
            state = SUSPICIOUS
        else:
            state = NORMAL
        return {"state": state, "reasons": reasons,
                "evidence": {"repeated_interactions": repeated,
                             "shared_target_groups": groups,
                             "reciprocal_pairs": reciprocal_pairs}}

    @staticmethod
    def _from_edges(edges: Any) -> dict[str, dict[str, int]]:
        result: dict[str, dict[str, int]] = {}
        if not isinstance(edges, list):
            return result
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            source, target, count = (edge.get("source_runtime_id"),
                                     edge.get("target_runtime_id"),
                                     edge.get("interaction_count"))
            if isinstance(source, str) and isinstance(target, str):
                result.setdefault(source, {})[target] = count
        return result


def assess_collusion(interactions: Any) -> dict[str, Any]:
    """Convenience wrapper for one-off assessments."""
    return CollusionAssessor().assess(interactions)
