"""Run the local SentinelAgent runtime with a durable security audit trail."""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from run_live_runtime import seed_missing_agents  # noqa: E402
from sentinelagent.live_dashboard import LiveDashboardSnapshotProvider  # noqa: E402
from sentinelagent.models import utc_now  # noqa: E402
from sentinelagent.persistent_audit import PersistentAuditTrail  # noqa: E402
from sentinelagent.registry import AgentRegistry  # noqa: E402
from sentinelagent.runtime_server import LocalSentinelRuntime, create_runtime_server  # noqa: E402


def _safe_text(value: Any, max_length: int = 512) -> str | None:
    if not isinstance(value, str):
        return None
    return value[:max_length]


class AuditedLocalSentinelRuntime(LocalSentinelRuntime):
    """Local runtime that durably records every valid edge authorization result."""

    def __init__(self, registry: AgentRegistry, audit_trail: PersistentAuditTrail, **kwargs: Any) -> None:
        super().__init__(registry, **kwargs)
        self.audit_trail = audit_trail
        # The provider must use the same pipeline and registry instances as the
        # request handler so the dashboard shows both live state and durable logs.
        self.dashboard_provider = LiveDashboardSnapshotProvider(
            self.pipeline,
            self.registry,
            audit_provider=self.audit_trail.list_recent,
        )

    def submit_edge_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            response = super().submit_edge_request(payload)
        except Exception as exc:
            self.audit_trail.record({
                "event_type": "EDGE_REQUEST_ERROR",
                "captured_at": utc_now(),
                "request_id": _safe_text(payload.get("request_id")),
                "edge_id": _safe_text(payload.get("edge_id")),
                "error_type": type(exc).__name__,
            })
            raise

        # Allowlist only: never store request bodies, credentials, or arbitrary
        # payload keys. This deliberately logs DENY as well as ALLOW outcomes.
        audit_event = {
            "event_type": "EDGE_AUTHORIZATION",
            "captured_at": utc_now(),
            "request_id": _safe_text(response.get("request_id")),
            "trace_id": _safe_text(payload.get("trace_id")),
            "edge_id": _safe_text(response.get("edge_id")),
            "source_runtime_id": _safe_text(payload.get("source_runtime_id")),
            "target_runtime_id": _safe_text(payload.get("target_runtime_id")),
            "action": _safe_text(payload.get("action")),
            "requested_permission": _safe_text(payload.get("requested_permission")),
            "resource_id": _safe_text(payload.get("resource_id")),
            "decision": _safe_text(response.get("decision")),
            "reason": _safe_text(response.get("reason")),
            "accepted": response.get("accepted") is True,
            "event_queued": response.get("event_queued") is True,
        }
        self.audit_trail.record(audit_event)
        return response


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=os.getenv("SENTINEL_DASHBOARD_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("SENTINEL_DASHBOARD_PORT", "8765")))
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--max-wait-seconds", type=float, default=1.0)
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path(os.getenv("SENTINEL_STATE_DIR", str(Path.home() / ".sentinelagent" / "runtime"))),
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=ROOT / "config" / "local-agents.example.json",
        help="JSON config used only to register missing local agents",
    )
    args = parser.parse_args()
    if args.batch_size <= 0 or args.max_wait_seconds <= 0:
        parser.error("--batch-size and --max-wait-seconds must be greater than zero")
    if not 0 <= args.port <= 65535:
        parser.error("--port must be between 0 and 65535")
    if args.host not in {"127.0.0.1", "localhost", "::1"}:
        logging.warning("Non-loopback bind requested. Add authentication/TLS before network exposure.")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args.state_dir.mkdir(parents=True, exist_ok=True)
    registry = AgentRegistry(database_path=args.state_dir / "agent-registry.sqlite3")
    audit = PersistentAuditTrail(args.state_dir / "audit-trail.sqlite3")
    server = None
    try:
        inserted = seed_missing_agents(registry, args.config.resolve())
        logging.info("Loaded registry; newly registered local agents=%d", inserted)
        runtime = AuditedLocalSentinelRuntime(
            registry,
            audit,
            batch_size=args.batch_size,
            max_wait_seconds=args.max_wait_seconds,
        )

        def audit_http_rejection(status: int, error_code: str) -> None:
            allowed_rejections = {
                (400, "invalid_content_length"),
                (400, "invalid_request_body_size"),
                (400, "invalid_edge_request"),
                (404, "not_found"),
                (413, "invalid_request_body_size"),
                (415, "content_type_must_be_application_json"),
            }

            if (status, error_code) not in allowed_rejections:
                return

            runtime.audit_trail.record(
                {
                    "event_type": "HTTP_REQUEST_REJECTED",
                    "captured_at": utc_now(),
                    "http_status": status,
                    "error_code": error_code,
                }
            )

        server = create_runtime_server(
            runtime,
            host=args.host,
            port=args.port,
            rejection_callback=audit_http_rejection,
        )
        logging.info(
            "Audited SentinelAgent runtime listening on http://%s:%d",
            args.host,
            server.server_address[1],
        )
        logging.info(
            "Durable security audit database: %s",
            args.state_dir / "audit-trail.sqlite3",
        )
        logging.info(
            "Dashboard: http://%s:%d/ | Submit edge events: POST /api/edge-requests",
            args.host,
            server.server_address[1],
        )
        server.serve_forever(poll_interval=0.1)

    except KeyboardInterrupt:
        logging.info("Shutdown requested")
    finally:
        if server is not None:
            server.server_close()
        audit.close()
        registry.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
