from typing import Any

from sentinelagent.analysis import AnalysisEngine
from sentinelagent.batch_worker import BatchWorker
from sentinelagent.cross_batch import CrossBatchAnalyzer
from sentinelagent.policy import PolicyDecision, PolicyEngine
from sentinelagent.policy_enforcer import PolicyEnforcer
from sentinelagent.registry import AgentRegistry


class AnalysisPipeline:
    """Connect analysis, cross-batch evidence, policy, and enforcement."""

    def __init__(self, registry: AgentRegistry | None = None) -> None:
        self.analysis_engine = AnalysisEngine()
        self.cross_batch_analyzer = CrossBatchAnalyzer()
        self.policy_engine = PolicyEngine()
        self.registry = registry
        self.policy_enforcer = (
            PolicyEnforcer(registry) if registry is not None else None
        )
        self.batch_worker = BatchWorker(
            batch_handler=self._handle_batch,
        )

    def _handle_batch(
        self,
        batch: list[Any],
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        """Analyze a batch, accumulate evidence, and evaluate policy."""
        analysis_result = self.analysis_engine.analyze(batch)

        self.cross_batch_analyzer.process_batch(
            analysis_result,
            batch_id=batch_id,
        )

        policy_decisions = self.policy_engine.evaluate_analysis(
            analysis_result
        )

        analysis_result["policy_decisions"] = policy_decisions
        analysis_result["policy_decisions_json"] = {
            runtime_id: decision.to_dict()
            for runtime_id, decision in policy_decisions.items()
        }

        if self.policy_enforcer is not None:
            analysis_result["enforced_actions"] = (
                self.policy_enforcer.enforce(policy_decisions)
            )
            analysis_result["replacement_runtime_ids"] = dict(
                self.policy_enforcer.replacement_runtime_ids
            )
        else:
            analysis_result["replacement_runtime_ids"] = {}

        return analysis_result

    def process(
        self,
        batch: list[Any],
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        """Process one batch through analysis and policy enforcement."""
        return self.batch_worker.process(
            batch,
            batch_id=batch_id,
        )

    def snapshot(self) -> dict[str, Any]:
        """Return accumulated evidence and policy decisions without side effects.

        Snapshotting is observational only. Enforcement belongs to batch
        processing so that reading system state cannot unexpectedly quarantine
        or replace a runtime.
        """
        snapshot = self.cross_batch_analyzer.snapshot()

        policy_decisions = self.policy_engine.evaluate(
            snapshot.get("runtime_risk_assessments", {})
        )
        snapshot["policy_decisions"] = policy_decisions
        snapshot["policy_decisions_json"] = {
            runtime_id: decision.to_dict()
            for runtime_id, decision in policy_decisions.items()
        }

        return snapshot
