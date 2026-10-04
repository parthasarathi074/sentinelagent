from dataclasses import dataclass
from enum import Enum
from typing import Any


class PolicyAction(str, Enum):
    MONITOR = "MONITOR"
    MARK_SUSPICIOUS = "MARK_SUSPICIOUS"
    QUARANTINE = "QUARANTINE"


@dataclass(frozen=True)
class PolicyDecision:
    runtime_agent_id: str
    risk_score: int
    risk_level: str
    action: PolicyAction
    reasons: tuple[str, ...]


class PolicyEngine:
    """Map analyzed runtime risk evidence to security policy actions."""

    def evaluate(
        self,
        runtime_risk_assessments: dict[str, Any],
    ) -> dict[str, PolicyDecision]:
        if not isinstance(runtime_risk_assessments, dict):
            raise ValueError("runtime_risk_assessments must be a dictionary")

        decisions: dict[str, PolicyDecision] = {}

        for runtime_agent_id, assessment in runtime_risk_assessments.items():
            if (
                not isinstance(runtime_agent_id, str)
                or not runtime_agent_id.strip()
            ):
                continue

            if not isinstance(assessment, dict):
                continue

            risk_score = assessment.get("risk_score", 0)
            risk_level = assessment.get("risk_level", "LOW")
            reasons = assessment.get("reasons", [])

            if (
                isinstance(risk_score, bool)
                or not isinstance(risk_score, int)
                or risk_score < 0
            ):
                raise ValueError(
                    f"Invalid risk score for runtime: {runtime_agent_id}"
                )

            if risk_level not in {"LOW", "MEDIUM", "HIGH"}:
                raise ValueError(
                    f"Invalid risk level for runtime: {runtime_agent_id}"
                )

            if not isinstance(reasons, list):
                raise ValueError(
                    f"Invalid risk reasons for runtime: {runtime_agent_id}"
                )

            normalized_reasons = tuple(
                reason
                for reason in reasons
                if isinstance(reason, str) and reason.strip()
            )

            if risk_level == "HIGH":
                action = PolicyAction.QUARANTINE
            elif risk_level == "MEDIUM":
                action = PolicyAction.MARK_SUSPICIOUS
            else:
                action = PolicyAction.MONITOR

            decisions[runtime_agent_id] = PolicyDecision(
                runtime_agent_id=runtime_agent_id,
                risk_score=risk_score,
                risk_level=risk_level,
                action=action,
                reasons=normalized_reasons,
            )

        return decisions

    def evaluate_analysis(
        self,
        analysis_result: dict[str, Any],
    ) -> dict[str, PolicyDecision]:
        """Evaluate the risk assessments contained in an analysis result."""
        if not isinstance(analysis_result, dict):
            raise ValueError("analysis_result must be a dictionary")

        return self.evaluate(
            analysis_result.get("runtime_risk_assessments", {})
        )
