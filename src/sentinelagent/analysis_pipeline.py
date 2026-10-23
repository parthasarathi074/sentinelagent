import logging
from typing import Any

from sentinelagent.analysis import AnalysisEngine
from sentinelagent.batch_worker import BatchWorker
from sentinelagent.collusion import CollusionAssessor
from sentinelagent.cross_batch import CrossBatchAnalyzer
from sentinelagent.edge import SentinelEdge
from sentinelagent.event_stream import EventStream
from sentinelagent.metrics import Metrics
from sentinelagent.micro_batch import MicroBatchProcessor
from sentinelagent.models import EdgeRequest, EdgeResult
from sentinelagent.policy import PolicyEngine
from sentinelagent.policy_enforcer import PolicyEnforcer
from sentinelagent.recovery import PipelineRecoveryManager
from sentinelagent.registry import AgentRegistry


logger = logging.getLogger(__name__)


class AnalysisPipeline:
    """Connect analysis, cross-batch evidence, policy, enforcement, and recovery."""

    def __init__(
        self,
        registry: AgentRegistry | None = None,
        *,
        event_stream: Any | None = None,
        batch_size: int = 100,
        max_wait_seconds: float = 1.0,
    ) -> None:
        self.analysis_engine = AnalysisEngine()
        self.cross_batch_analyzer = CrossBatchAnalyzer()
        self.policy_engine = PolicyEngine()
        self.registry = registry
        self.metrics = Metrics()
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
        self.event_stream = (
            event_stream if event_stream is not None else EventStream()
        )
        self.sentinel_edge = (
            SentinelEdge(
                registry=registry,
                event_sink=self.event_stream.publish,
            )
            if registry is not None
            else None
        )
        self._last_ingested_result: dict[str, Any] | None = None
        self.micro_batch_processor = MicroBatchProcessor(
            event_stream=self.event_stream,
            batch_size=batch_size,
            batch_handler=self._process_ingested_batch,
            batch_handler_with_id=self._process_ingested_batch_with_id,
            max_wait_seconds=max_wait_seconds,
            on_retry=self.metrics.record_retried,
        )

    def handle_edge_request(self, request: EdgeRequest) -> EdgeResult:
        """Authorize an inter-agent request and queue its security event."""
        if self.sentinel_edge is None:
            raise RuntimeError(
                "An AgentRegistry is required to enable edge request handling"
            )
        return self.sentinel_edge.handle(request)

    def process_pending_events(self) -> dict[str, Any] | None:
        """Process one queued micro-batch when a batching condition is met."""
        self._last_ingested_result = None
        batch = self.micro_batch_processor.process_once()
        if not batch:
            return None
        return self._last_ingested_result

    def _process_ingested_batch(self, batch: list[Any]) -> None:
        self._last_ingested_result = self.process(batch)

    def _process_ingested_batch_with_id(
        self,
        batch: list[Any],
        batch_id: str,
    ) -> None:
        self._last_ingested_result = self.process(
            batch,
            batch_id=batch_id,
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

        cross_batch_risk_assessments = self.cross_batch_analyzer.snapshot().get(
            "runtime_risk_assessments", {}
        )
        analysis_result["cross_batch_risk_assessments"] = (
            cross_batch_risk_assessments
        )
        policy_analysis_result = dict(analysis_result)
        policy_analysis_result["runtime_risk_assessments"] = (
            cross_batch_risk_assessments
        )
        policy_decisions = self.policy_engine.evaluate_analysis(
            policy_analysis_result
        )

        for runtime_agent_id, decision in policy_decisions.items():
            logger.info(
                (
                    "sentinel_policy_decision batch_id=%r "
                    "runtime_agent_id=%r risk_score=%d risk_level=%s "
                    "action=%s reasons=%r"
                ),
                batch_id,
                runtime_agent_id,
                decision.risk_score,
                decision.risk_level,
                decision.action.value,
                decision.reasons,
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

            for runtime_agent_id, action in analysis_result[
                "enforced_actions"
            ].items():
                enforcement_status = {
                    "SKIPPED_UNKNOWN_RUNTIME": "skipped_unknown_runtime",
                    "ALREADY_QUARANTINED": "already_quarantined",
                }.get(action, "applied")
                log_method = (
                    logger.warning
                    if action == "SKIPPED_UNKNOWN_RUNTIME"
                    else logger.info
                )
                log_method(
                    (
                        "sentinel_policy_enforcement batch_id=%r "
                        "runtime_agent_id=%r action=%s status=%s "
                        "replacement_runtime_id=%r"
                    ),
                    batch_id,
                    runtime_agent_id,
                    action,
                    enforcement_status,
                    analysis_result["replacement_runtime_ids"].get(
                        runtime_agent_id
                    ),
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

                    if recovery_record.attempt > 1:
                        self.metrics.record_retried()

                    if recovery_record.state.value == "FAILED":
                        self.metrics.record_failed()

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
        try:
            result = self.batch_worker.process(
                batch,
                batch_id=batch_id,
            )
        except Exception as exc:
            self.metrics.record_failed()
            logger.error(
                "sentinel_pipeline_batch_failed batch_id=%r event_count=%d error_type=%s",
                batch_id,
                len(batch),
                type(exc).__name__,
            )
            raise

        self.metrics.record_processed(len(batch))

        enforced_actions = result.get("enforced_actions", {})
        if isinstance(enforced_actions, dict):
            quarantined_count = sum(
                1
                for action in enforced_actions.values()
                if action == "QUARANTINE"
            )
            if quarantined_count:
                self.metrics.record_quarantined(quarantined_count)

        return result

    def snapshot(self) -> dict[str, Any]:
        """Return accumulated evidence and policy decisions without side effects."""
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

    def metrics_snapshot(self) -> dict[str, int]:
        """Return a snapshot of pipeline processing counters."""
        return self.metrics.snapshot().to_dict()
