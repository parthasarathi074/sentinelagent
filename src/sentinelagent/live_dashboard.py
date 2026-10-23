"""Adapt live SentinelAgent pipeline and registry state for the read-only dashboard.

Use this provider in the same process that owns the AnalysisPipeline and
AgentRegistry. A separate process cannot inspect their in-memory state.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from sentinelagent.dashboard import create_server
from sentinelagent.models import AgentStatus

AuditProvider = Callable[[], list[dict[str, Any]]]


def _json_safe(value: Any) -> Any:
    """Convert common SentinelAgent values to JSON-safe primitive values."""
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _json_safe(value.to_dict())
    if hasattr(value, "value") and isinstance(value.value, (str, int, float, bool)):
        return value.value
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_json_safe(item) for item in value), key=str)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _status_value(status: Any) -> str:
    value = getattr(status, "value", status)
    return str(value)


class LiveDashboardSnapshotProvider:
    """Read current live state from the application's existing objects.

    ``audit_provider`` is optional because the current registry/pipeline do not
    expose a durable general-purpose audit log. When omitted, ``audit_trail``
    remains empty and ``audit_trail_configured`` is false rather than
    fabricating historical events.
    """

    def __init__(
        self,
        pipeline: Any,
        registry: Any | None = None,
        *,
        event_stream: Any | None = None,
        audit_provider: AuditProvider | None = None,
    ) -> None:
        if not callable(getattr(pipeline, "snapshot", None)):
            raise TypeError("pipeline must provide snapshot()")
        if not callable(getattr(pipeline, "metrics_snapshot", None)):
            raise TypeError("pipeline must provide metrics_snapshot()")

        selected_registry = registry
        if selected_registry is None:
            selected_registry = getattr(pipeline, "registry", None)
        if selected_registry is None or not callable(
            getattr(selected_registry, "all_agents", None)
        ):
            raise TypeError("registry must provide all_agents()")

        pipeline_registry = getattr(pipeline, "registry", None)
        if pipeline_registry is not None and pipeline_registry is not selected_registry:
            raise ValueError(
                "Pass the same AgentRegistry instance used by AnalysisPipeline"
            )

        self.pipeline = pipeline
        self.registry = selected_registry
        self.event_stream = (
            event_stream
            if event_stream is not None
            else getattr(pipeline, "event_stream", None)
        )
        self.audit_provider = audit_provider

    def __call__(self) -> dict[str, Any]:
        """Return a current dashboard snapshot; do not mutate pipeline state."""
        captured_at = datetime.now(timezone.utc).isoformat()
        pipeline_state = self.pipeline.snapshot()
        if not isinstance(pipeline_state, dict):
            raise TypeError("pipeline.snapshot() must return a dictionary")

        agents = []
        for agent in self.registry.all_agents():
            status = _status_value(agent.status)
            # Explicit allowlist: credentials references and other secrets are
            # intentionally excluded from dashboard responses.
            agents.append({
                "logical_agent_id": agent.logical_agent_id,
                "runtime_agent_id": agent.runtime_agent_id,
                "role": agent.role,
                "version": agent.version,
                "status": status,
                "permissions": sorted(agent.permissions),
                "allowed_targets": sorted(agent.allowed_targets),
                "created_at": agent.created_at,
                "activated_at": agent.activated_at,
                "quarantined_at": agent.quarantined_at,
                "replacement_for": agent.replacement_for,
                "replaced_by": agent.replaced_by,
            })

        risk_assessments = _json_safe(
            pipeline_state.get("runtime_risk_assessments", {})
        )
        policy_decisions = _json_safe(
            pipeline_state.get("policy_decisions_json", {})
        )
        agent_by_runtime = {
            agent["runtime_agent_id"]: agent for agent in agents
        }

        suspicious_activity = []
        for runtime_id, assessment in risk_assessments.items():
            if not isinstance(assessment, dict):
                continue
            score = assessment.get("risk_score", 0)
            level = str(assessment.get("risk_level", "LOW"))
            reasons = assessment.get("reasons", [])
            current_agent = agent_by_runtime.get(runtime_id, {})
            if (
                (isinstance(score, (int, float)) and score > 0)
                or level.upper() not in {"LOW", "NORMAL", "NONE"}
                or current_agent.get("status") in {
                    AgentStatus.SUSPICIOUS.value,
                    AgentStatus.QUARANTINED.value,
                }
            ):
                suspicious_activity.append({
                    "runtime_agent_id": runtime_id,
                    "risk_score": score,
                    "risk_level": level,
                    "reasons": reasons,
                    "agent_status": current_agent.get("status"),
                })

        # These are current-state summaries, not an immutable event history.
        quarantined_agents = [
            {
                "runtime_agent_id": agent["runtime_agent_id"],
                "logical_agent_id": agent["logical_agent_id"],
                "status": agent["status"],
                "quarantined_at": agent["quarantined_at"],
            }
            for agent in agents
            if agent["status"] == AgentStatus.QUARANTINED.value
        ]
        replacement_relations = [
            {
                "runtime_agent_id": agent["runtime_agent_id"],
                "logical_agent_id": agent["logical_agent_id"],
                "replacement_for": agent["replacement_for"],
                "replaced_by": agent["replaced_by"],
                "status": agent["status"],
            }
            for agent in agents
            if agent["replacement_for"] or agent["replaced_by"]
        ]

        # Policy decisions represent the latest decision derived from the
        # current cumulative evidence. The pipeline does not retain a durable
        # history of all decisions, so label each item accordingly.
        security_events = []
        if isinstance(policy_decisions, dict):
            for runtime_id, decision in policy_decisions.items():
                if not isinstance(decision, dict):
                    continue
                security_events.append({
                    "event_type": "CURRENT_POLICY_DECISION",
                    "captured_at": captured_at,
                    "runtime_agent_id": runtime_id,
                    **decision,
                })

        audit_trail: list[dict[str, Any]] = []
        if self.audit_provider is not None:
            supplied_audit = self.audit_provider()
            if not isinstance(supplied_audit, list) or any(
                not isinstance(item, dict) for item in supplied_audit
            ):
                raise TypeError("audit_provider must return a list of dictionaries")
            audit_trail = _json_safe(supplied_audit)

        queue_state: dict[str, Any] = {"available": False}
        size_method = getattr(self.event_stream, "size", None)
        if callable(size_method):
            queue_state = {"available": True, "pending_events": int(size_method())}

        collusion = _json_safe(pipeline_state.get("collusion_assessment", {}))
        return {
            "status": "ok",
            "captured_at": captured_at,
            "metrics": _json_safe(self.pipeline.metrics_snapshot()),
            "agent_status": agents,
            "risk_state": {
                "runtime_risk_assessments": risk_assessments,
                "collusion_assessment": collusion,
            },
            "suspicious_activity": suspicious_activity,
            "security_events": security_events,
            "interaction_relationships": _json_safe(
                pipeline_state.get("runtime_target_interactions", {})
            ),
            "quarantine_events": quarantined_agents,
            "replacement_events": replacement_relations,
            "recovery_state": _json_safe(
                pipeline_state.get("recovery_results", {})
            ),
            "audit_trail": audit_trail,
            "audit_trail_configured": self.audit_provider is not None,
            "event_queue": queue_state,
            "batches_processed": pipeline_state.get("batches_processed", 0),
        }


def create_live_dashboard_server(
    pipeline: Any,
    registry: Any | None = None,
    *,
    event_stream: Any | None = None,
    audit_provider: AuditProvider | None = None,
    host: str = "127.0.0.1",
    port: int = 8765,
):
    """Create an HTTP server that reads the supplied live objects."""
    provider = LiveDashboardSnapshotProvider(
        pipeline,
        registry,
        event_stream=event_stream,
        audit_provider=audit_provider,
    )
    return create_server(provider, host=host, port=port)
