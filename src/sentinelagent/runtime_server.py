"""Local development runtime that connects edge requests to the live dashboard.

This module composes existing SentinelAgent objects; it does not modify the
pipeline, registry, or dashboard implementation. Bind to loopback unless a
separate authenticated deployment layer is provided.
"""
from __future__ import annotations

import json
import logging
from http.server import HTTPServer
from typing import Any
from urllib.parse import urlparse

from sentinelagent.analysis_pipeline import AnalysisPipeline
from sentinelagent.dashboard import make_handler
from sentinelagent.live_dashboard import LiveDashboardSnapshotProvider
from sentinelagent.models import EdgeRequest
from sentinelagent.registry import AgentRegistry

logger = logging.getLogger(__name__)
MAX_REQUEST_BYTES = 64 * 1024
EDGE_REQUEST_FIELDS = {
    "request_id",
    "trace_id",
    "edge_id",
    "source_runtime_id",
    "target_runtime_id",
    "action",
    "requested_permission",
    "resource_id",
    "payload_hash",
    "timestamp",
    "protocol_version",
}


class LocalSentinelRuntime:
    """Own the shared pipeline/registry instances used by API and dashboard."""

    def __init__(
        self,
        registry: AgentRegistry,
        *,
        batch_size: int = 10,
        max_wait_seconds: float = 1.0,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than zero")
        if max_wait_seconds <= 0:
            raise ValueError("max_wait_seconds must be greater than zero")
        self.registry = registry
        # The stream is intentionally the existing in-memory EventStream in
        # this local runner. Use a durable broker/stream adapter for production.
        self.pipeline = AnalysisPipeline(
            registry=registry,
            batch_size=batch_size,
            max_wait_seconds=max_wait_seconds,
        )
        self.dashboard_provider = LiveDashboardSnapshotProvider(
            self.pipeline,
            self.registry,
        )

    def submit_edge_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Validate and route a JSON payload through the real SentinelEdge."""
        unknown_fields = set(payload) - EDGE_REQUEST_FIELDS
        if unknown_fields:
            raise ValueError(
                "Unsupported field(s): " + ", ".join(sorted(unknown_fields))
            )
        try:
            request = EdgeRequest(**payload)
        except TypeError as exc:
            raise ValueError(f"Invalid EdgeRequest fields: {exc}") from exc

        result = self.pipeline.handle_edge_request(request)
        return {
            "accepted": True,
            "request_id": request.request_id,
            "edge_id": request.edge_id,
            "decision": result.decision.value,
            "reason": result.reason,
            "event_queued": True,
        }

    def process_pending_events(self) -> dict[str, Any] | None:
        """Process at most one ready batch using the configured batch window."""
        return self.pipeline.process_pending_events()


class _RuntimeHTTPServer(HTTPServer):
    """Single-threaded server so SQLite registry access stays thread-local."""

    def __init__(self, address, handler, runtime: LocalSentinelRuntime):
        self.runtime = runtime
        super().__init__(address, handler)

    def service_actions(self) -> None:
        # Called by serve_forever on the same thread as registry/API access.
        # It honors the pipeline's batch size and time-window conditions.
        try:
            self.runtime.process_pending_events()
        except Exception as exc:  # pipeline records the failure and restores the batch
            logger.error(
                "sentinel_runtime_batch_error error_type=%s",
                type(exc).__name__,
            )


def create_runtime_server(
    runtime: LocalSentinelRuntime,
    host: str = "127.0.0.1",
    port: int = 8765,
) -> HTTPServer:
    """Create the dashboard plus POST /api/edge-requests local endpoint."""
    if not (0 <= port <= 65535):
        raise ValueError("port must be between 0 and 65535")
    BaseHandler = make_handler(runtime.dashboard_provider)

    class RuntimeHandler(BaseHandler):
        def do_POST(self) -> None:  # noqa: N802 - stdlib handler contract
            path = urlparse(self.path).path
            if path != "/api/edge-requests":
                self._send(404, {"error": "not_found"})
                return

            content_type = self.headers.get("Content-Type", "")
            if "application/json" not in content_type.lower():
                self._send(415, {"error": "content_type_must_be_application_json"})
                return

            raw_length = self.headers.get("Content-Length")
            try:
                length = int(raw_length or "0")
            except ValueError:
                self._send(400, {"error": "invalid_content_length"})
                return
            if length <= 0 or length > MAX_REQUEST_BYTES:
                self._send(413 if length > MAX_REQUEST_BYTES else 400,
                           {"error": "invalid_request_body_size"})
                return

            try:
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
                if not isinstance(payload, dict):
                    raise ValueError("JSON body must be an object")
                response = runtime.submit_edge_request(payload)
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                self._send(400, {"error": "invalid_edge_request", "detail": str(exc)})
                return
            except Exception as exc:
                logger.error(
                    "sentinel_runtime_request_error error_type=%s",
                    type(exc).__name__,
                )
                self._send(500, {"error": "edge_request_unavailable"})
                return

            self._send(202, response)

        def log_message(self, fmt: str, *args) -> None:
            # Never echo request bodies, which could contain sensitive metadata.
            return

    return _RuntimeHTTPServer((host, port), RuntimeHandler, runtime)
