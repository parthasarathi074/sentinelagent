from typing import Any

from sentinelagent.analysis import AnalysisEngine
from sentinelagent.batch_worker import BatchWorker
from sentinelagent.cross_batch import CrossBatchAnalyzer
from sentinelagent.collusion import CollusionAssessor
from sentinelagent.policy import PolicyDecision, PolicyEngine
from sentinelagent.policy_enforcer import PolicyEnforcer
from sentinelagent.recovery import PipelineRecoveryManager
from sentinelagent.registry import AgentRegistry


class AnalysisPipeline:
    """Connect analysis, cross-batch evidence, policy, enforcement, and recovery."""

    def __init__(self, registry: AgentRegistry | None = None) -> None:
        self.analysis_engine = AnalysisEngine()
        self.cross_batch_analyzer = CrossBatchAnalyzer()
        self.policy_engine = PolicyEngine()
        self.registry = registry
        self.policy_enforcer = (
            PolicyEnforcer(registry) if registry is not None else None
        )
        self.recovery_manager = (
            PipelineRecoveryManager(registry)
            if registry is not None
            else None
        )
        self.batch_worker = BatchWorker(
            batch_handler=self._handle_batch,
        )

    def _handle_batch(
        self,
        batch: list[Any],
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        """Analyze a batch, accumulate evidence, evaluate policy, and recover replacements."""
        analysis_result = self.analysis_engine.analyze(batch)

        self.cross_batch_analyzer.process_batch(
            analysis_result,
            batch_id=batch_id,
        )

        # Collusion assessment is observational only; existing policy decisions
        # continue to be based on the established risk engine.
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

            recovery_results: dict[str, dict[str, object]] = {}

            if self.recovery_manager is not None:
                for (
                    old_runtime_id,
                    replacement_runtime_id,
                ) in self.policy_enforcer.replacement_runtime_ids.items():
                    recovery_record = self.recovery_manager.recover(
                        old_runtime_id=old_runtime_id,
                        replacement_runtime_id=replacement_runtime_id,
                    )
                    recovery_results[old_runtime_id] = (
                        recovery_record.to_dict()
                    )

            analysis_result["recovery_results"] = recovery_results
        else:
            analysis_result["replacement_runtime_ids"] = {}
            analysis_result["recovery_results"] = {}

        return analysis_result

    def process(
        self,
        batch: list[Any],
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        """Process one batch through analysis, policy, enforcement, and recovery."""
        return self.batch_worker.process(
            batch,
            batch_id=batch_id,
        )

    def snapshot(self) -> dict[str, Any]:
        """Return accumulated evidence and policy decisions without side effects.

        Snapshotting is observational only. Enforcement and recovery belong to
        batch processing so that reading system state cannot unexpectedly
        quarantine, replace, or recover a runtime.
        """
        snapshot = self.cross_batch_analyzer.snapshot()
        snapshot["collusion_assessment"] = CollusionAssessor().assess(
            snapshot.get("runtime_target_interactions", {})
        )

        policy_decisions = self.policy_engine.evaluate(
            snapshot.get("runtime_risk_assessments", {})
        )
        snapshot["policy_decisions"] = policy_decisions
        snapshot["policy_decisions_json"] = {
            runtime_id: decision.to_dict()
            for runtime_id, decision in policy_decisions.items()
        }

        if self.recovery_manager is not None:
            snapshot["recovery_results"] = (
                self.recovery_manager.snapshot_json()
            )
        else:
            snapshot["recovery_results"] = {}

        return snapshot
