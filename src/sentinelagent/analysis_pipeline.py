from typing import Any

from sentinelagent.analysis import AnalysisEngine
from sentinelagent.batch_worker import BatchWorker
from sentinelagent.cross_batch import CrossBatchAnalyzer
from sentinelagent.policy import PolicyDecision, PolicyEngine


class AnalysisPipeline:
    """Connect per-batch analysis, cross-batch evidence, and policy evaluation."""

    def __init__(self) -> None:
        self.analysis_engine = AnalysisEngine()
        self.cross_batch_analyzer = CrossBatchAnalyzer()
        self.policy_engine = PolicyEngine()
        self.batch_worker = BatchWorker(
            batch_handler=self._handle_batch,
        )

    def _handle_batch(
        self,
        batch: list[Any],
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        """Analyze a batch and accumulate its evidence."""
        analysis_result = self.analysis_engine.analyze(batch)
        self.cross_batch_analyzer.process_batch(
            analysis_result,
            batch_id=batch_id,
        )

        policy_decisions = self.policy_engine.evaluate_analysis(
            analysis_result
        )

        analysis_result["policy_decisions"] = policy_decisions
        return analysis_result

    def process(
        self,
        batch: list[Any],
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        """Process one batch and return analysis plus policy decisions."""
        return self.batch_worker.process(
            batch,
            batch_id=batch_id,
        )

    def snapshot(self) -> dict[str, Any]:
        """Return the current cross-batch evidence snapshot."""
        snapshot = self.cross_batch_analyzer.snapshot()
        snapshot["policy_decisions"] = self.policy_engine.evaluate(
            snapshot.get("runtime_risk_assessments", {})
        )
        return snapshot
