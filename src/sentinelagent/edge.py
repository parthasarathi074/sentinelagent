from __future__ import annotations

from typing import Any, Callable

from .models import EdgeDecision, EdgeRequest, EdgeResult
from .registry import AgentRegistry


class SentinelEdge:
    """
    Inline security checkpoint for one logical source-target relationship.

    The edge performs fast-path security checks.
    Heavy behavioral and collusion analysis happens later.
    """

    def __init__(
        self,
        registry: AgentRegistry,
        event_sink: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.registry = registry
        self.event_sink = event_sink

    def handle(self, request: EdgeRequest) -> EdgeResult:
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
                "logical_id": (
                    source.logical_agent_id if source else None
                ),
                "runtime_id": (
                    source.runtime_agent_id if source else None
                ),
                "status": source.status.value if source else None,
            },
            "target_agent": {
                "logical_id": (
                    target.logical_agent_id if target else None
                ),
                "runtime_id": (
                    target.runtime_agent_id if target else None
                ),
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
        if self.event_sink is not None:
            self.event_sink(event)