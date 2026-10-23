from __future__ import annotations

import logging
from typing import Any, Callable

from .models import EdgeDecision, EdgeRequest, EdgeResult
from .registry import AgentRegistry


logger = logging.getLogger(__name__)


class SentinelEdge:
    """
    Inline security checkpoint for one logical source-target relationship.

    The edge performs fast-path security checks.
    Heavy behavioral and collusion analysis happens later.
    """

    _REQUIRED_STRING_FIELDS = (
        "request_id",
        "trace_id",
        "edge_id",
        "source_runtime_id",
        "target_runtime_id",
        "action",
        "requested_permission",
        "timestamp",
        "protocol_version",
    )

    def __init__(
        self,
        registry: AgentRegistry,
        event_sink: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.registry = registry
        self.event_sink = event_sink

    def handle(self, request: EdgeRequest) -> EdgeResult:
        validation_error = self._validate_request(request)

        if validation_error is not None:
            result = self._deny_invalid_request(request, validation_error)
            self._publish(result.event)
            return result

        try:
            source = self.registry.get(request.source_runtime_id)
            target = self.registry.get(request.target_runtime_id)
        except KeyError as exc:
            result = self._deny(
                request=request,
                reason=str(exc),
                source=None,
                target=None,
            )
            self._publish(result.event)
            return result

        # Preserve the registry's authorization decision and explanation.
        authorized, reason = self.registry.is_authorized(
            source_runtime_id=request.source_runtime_id,
            target_runtime_id=request.target_runtime_id,
            requested_permission=request.requested_permission,
        )

        if not authorized:
            result = self._deny(
                request=request,
                reason=reason,
                source=source,
                target=target,
            )
        else:
            result = self._allow(
                request=request,
                source=source,
                target=target,
            )

        self._publish(result.event)
        return result

    @classmethod
    def _validate_request(cls, request: Any) -> str | None:
        if not isinstance(request, EdgeRequest):
            return "Invalid request: expected an EdgeRequest"

        for field in cls._REQUIRED_STRING_FIELDS:
            value = getattr(request, field, None)
            if not isinstance(value, str) or not value.strip():
                return (
                    f"Invalid request: {field} "
                    "must be a non-empty string"
                )

        for field in ("resource_id", "payload_hash"):
            value = getattr(request, field, None)
            if value is not None and not isinstance(value, str):
                return (
                    f"Invalid request: {field} "
                    "must be a string or None"
                )

        return None

    def _deny_invalid_request(
        self,
        request: Any,
        reason: str,
    ) -> EdgeResult:
        def safe_value(field: str) -> str | None:
            value = getattr(request, field, None)
            return value if isinstance(value, str) else None

        event = {
            "event_id": safe_value("request_id") or "invalid-request",
            "event_type": "INTER_AGENT_REQUEST",
            "schema_version": safe_value("protocol_version") or "1.0",
            "timestamp": safe_value("timestamp"),
            "edge_id": safe_value("edge_id"),
            "request_id": safe_value("request_id"),
            "trace_id": safe_value("trace_id"),
            "source_agent": {
                "logical_id": None,
                "runtime_id": safe_value("source_runtime_id"),
                "status": None,
            },
            "target_agent": {
                "logical_id": None,
                "runtime_id": safe_value("target_runtime_id"),
                "status": None,
            },
            "action": safe_value("action"),
            "requested_permission": safe_value("requested_permission"),
            "resource_id": safe_value("resource_id"),
            "payload_hash": safe_value("payload_hash"),
            "decision": EdgeDecision.DENY.value,
            "decision_reason": reason,
        }

        return EdgeResult(
            decision=EdgeDecision.DENY,
            reason=reason,
            event=event,
        )

    def _allow(
        self,
        request: EdgeRequest,
        source: Any,
        target: Any,
    ) -> EdgeResult:
        event = self._event(
            request=request,
            source=source,
            target=target,
            decision=EdgeDecision.ALLOW,
            reason="authorized",
        )

        return EdgeResult(
            decision=EdgeDecision.ALLOW,
            reason="authorized",
            event=event,
        )

    def _deny(
        self,
        request: EdgeRequest,
        reason: str,
        source: Any,
        target: Any,
    ) -> EdgeResult:
        event = self._event(
            request=request,
            source=source,
            target=target,
            decision=EdgeDecision.DENY,
            reason=reason,
        )

        return EdgeResult(
            decision=EdgeDecision.DENY,
            reason=reason,
            event=event,
        )

    @staticmethod
    def _event(
        request: EdgeRequest,
        source: Any,
        target: Any,
        decision: EdgeDecision,
        reason: str,
    ) -> dict[str, Any]:
        return {
            "event_id": request.request_id,
            "event_type": "INTER_AGENT_REQUEST",
            "schema_version": request.protocol_version,
            "timestamp": request.timestamp,
            "edge_id": request.edge_id,
            "request_id": request.request_id,
            "trace_id": request.trace_id,
            "source_agent": {
                "logical_id": source.logical_agent_id if source else None,
                "runtime_id": request.source_runtime_id,
                "status": source.status.value if source else None,
            },
            "target_agent": {
                "logical_id": target.logical_agent_id if target else None,
                "runtime_id": request.target_runtime_id,
                "status": target.status.value if target else None,
            },
            "action": request.action,
            "requested_permission": request.requested_permission,
            "resource_id": request.resource_id,
            "payload_hash": request.payload_hash,
            "decision": decision.value,
            "decision_reason": reason,
        }

    def _publish(self, event: dict[str, Any]) -> None:
        source_agent = event.get("source_agent")
        target_agent = event.get("target_agent")
        source_runtime_id = (
            source_agent.get("runtime_id")
            if isinstance(source_agent, dict)
            else None
        )
        target_runtime_id = (
            target_agent.get("runtime_id")
            if isinstance(target_agent, dict)
            else None
        )

        logger.info(
            (
                "sentinel_edge_decision request_id=%r trace_id=%r edge_id=%r "
                "source_runtime_id=%r target_runtime_id=%r decision=%r "
                "reason=%r"
            ),
            event.get("request_id"),
            event.get("trace_id"),
            event.get("edge_id"),
            source_runtime_id,
            target_runtime_id,
            event.get("decision"),
            event.get("decision_reason"),
        )

        if self.event_sink is not None:
            try:
                self.event_sink(event)
            except Exception as exc:
                logger.error(
                    (
                        "sentinel_edge_event_publish_failed request_id=%r "
                        "trace_id=%r edge_id=%r error_type=%s"
                    ),
                    event.get("request_id"),
                    event.get("trace_id"),
                    event.get("edge_id"),
                    type(exc).__name__,
                )
                raise
