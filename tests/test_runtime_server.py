from __future__ import annotations

import json
import threading
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from sentinelagent.models import AgentRecord
from sentinelagent.registry import AgentRegistry
from sentinelagent.runtime_server import LocalSentinelRuntime, create_runtime_server
from sentinelagent.persistent_audit import PersistentAuditTrail


def make_registry() -> AgentRegistry:
    registry = AgentRegistry()
    registry.register(AgentRecord(
        logical_agent_id="agent-A", runtime_agent_id="runtime-A-001",
        role="ingest", version="1.0", permissions={"message:send"},
        allowed_targets={"agent-B"},
    ))
    registry.register(AgentRecord(
        logical_agent_id="agent-B", runtime_agent_id="runtime-B-001",
        role="validator", version="1.0", permissions={"validation:execute"},
        allowed_targets={"agent-A"},
    ))
    registry.activate("runtime-A-001")
    registry.activate("runtime-B-001")
    return registry


def post_json(url: str, payload: dict):
    request = Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    return urlopen(request, timeout=2)


def test_runtime_accepts_edge_event_and_dashboard_reads_actual_pipeline():
    registry = make_registry()
    runtime = LocalSentinelRuntime(registry, batch_size=1)
    server = create_runtime_server(runtime, port=0)
    thread = threading.Thread(
        target=lambda: server.serve_forever(poll_interval=0.01), daemon=True
    )
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        with post_json(base + "/api/edge-requests", {
            "request_id": "req-runtime-001",
            "trace_id": "trace-runtime-001",
            "edge_id": "edge-agent-A-agent-B",
            "source_runtime_id": "runtime-A-001",
            "target_runtime_id": "runtime-B-001",
            "action": "SEND_MESSAGE",
            "requested_permission": "message:send",
        }) as response:
            result = json.load(response)
            assert response.status == 202
            assert result["decision"] == "ALLOW"

        # The HTTP server processes one batch in service_actions after the POST.
        with urlopen(base + "/api/metrics", timeout=2) as response:
            metrics = json.load(response)["metrics"]
            assert metrics["processed"] == 1

        with urlopen(base + "/api/snapshot", timeout=2) as response:
            snapshot = json.load(response)
        assert {agent["runtime_agent_id"] for agent in snapshot["agent_status"]} == {
            "runtime-A-001", "runtime-B-001"
        }
        assert snapshot["event_queue"]["pending_events"] == 0
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        registry.close()


def test_runtime_rejects_unknown_fields_and_unknown_post_routes():
    registry = make_registry()
    runtime = LocalSentinelRuntime(registry, batch_size=1)
    server = create_runtime_server(runtime, port=0)
    thread = threading.Thread(
        target=lambda: server.serve_forever(poll_interval=0.01), daemon=True
    )
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        payload = {
            "request_id": "req-bad", "trace_id": "trace-bad",
            "edge_id": "edge", "source_runtime_id": "runtime-A-001",
            "target_runtime_id": "runtime-B-001", "action": "SEND_MESSAGE",
            "requested_permission": "message:send", "credentials_reference": "secret",
        }
        try:
            post_json(base + "/api/edge-requests", payload)
        except HTTPError as exc:
            assert exc.code == 400
            assert json.load(exc)["error"] == "invalid_edge_request"
        else:
            raise AssertionError("unknown fields should be rejected")

        try:
            post_json(base + "/other", {})
        except HTTPError as exc:
            assert exc.code == 404
        else:
            raise AssertionError("unknown POST route should be rejected")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        registry.close()


def test_config_seeds_only_missing_agents_without_overwriting_existing(tmp_path):
    from scripts.run_live_runtime import seed_missing_agents

    registry = AgentRegistry()
    registry.register(AgentRecord(
        logical_agent_id="existing-agent", runtime_agent_id="existing-runtime",
        role="original-role", version="9.0", permissions={"keep"},
        allowed_targets={"nobody"},
    ))
    registry.activate("existing-runtime")

    config = tmp_path / "agents.json"
    config.write_text(json.dumps({"agents": [
        {
            "logical_agent_id": "existing-agent",
            "runtime_agent_id": "existing-runtime",
            "role": "must-not-replace",
            "version": "1.0",
            "permissions": ["replace"],
            "allowed_targets": [],
        },
        {
            "logical_agent_id": "new-agent",
            "runtime_agent_id": "new-runtime",
            "role": "worker",
            "version": "1.0",
            "permissions": ["task:run"],
            "allowed_targets": ["existing-agent"],
        },
    ]}), encoding="utf-8")

    assert seed_missing_agents(registry, config) == 1
    existing = registry.get("existing-runtime")
    assert existing.role == "original-role"
    assert existing.permissions == {"keep"}
    assert registry.get("new-runtime").status.value == "ACTIVE"
    registry.close()

def test_rejected_request_audit_callback_receives_only_safe_metadata():
    registry = make_registry()
    runtime = LocalSentinelRuntime(registry, batch_size=1)
    recorded_rejections = []

    def audit_rejection(status, error_code):
        recorded_rejections.append({
            "status": status,
            "error_code": error_code,
        })

    server = create_runtime_server(
        runtime, port=0, rejection_callback=audit_rejection
    )
    thread = threading.Thread(
        target=lambda: server.serve_forever(poll_interval=0.01), daemon=True
    )
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"

    try:
        payload = {
            "request_id": "req-bad",
            "trace_id": "trace-bad",
            "edge_id": "edge",
            "source_runtime_id": "runtime-A-001",
            "target_runtime_id": "runtime-B-001",
            "action": "SEND_MESSAGE",
            "requested_permission": "message:send",
            "credentials_reference": "SECRET_TEST_CREDENTIAL",
            "raw_body": "SECRET_TEST_BODY",
        }

        try:
            post_json(base + "/api/edge-requests", payload)
        except HTTPError as exc:
            assert exc.code == 400
            exc.close()
        else:
            raise AssertionError("Unknown fields should be rejected")

        assert recorded_rejections == [{
            "status": 400,
            "error_code": "invalid_edge_request",
        }]
        assert "SECRET_TEST_CREDENTIAL" not in repr(recorded_rejections)
        assert "SECRET_TEST_BODY" not in repr(recorded_rejections)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        registry.close()

def test_rejection_audit_callback_failure_does_not_break_http_response(caplog):
    registry = make_registry()
    runtime = LocalSentinelRuntime(registry, batch_size=1)

    def failing_audit_callback(status, error_code):
        raise RuntimeError("Simulated audit failure")

    server = create_runtime_server(
        runtime, port=0, rejection_callback=failing_audit_callback
    )
    thread = threading.Thread(
        target=lambda: server.serve_forever(poll_interval=0.01), daemon=True
    )
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"

    try:
        payload = {
            "request_id": "req-bad",
            "trace_id": "trace-bad",
            "edge_id": "edge",
            "source_runtime_id": "runtime-A-001",
            "target_runtime_id": "runtime-B-001",
            "action": "SEND_MESSAGE",
            "requested_permission": "message:send",
            "credentials_reference": "SECRET_TEST_CREDENTIAL",
        }

        try:
            post_json(base + "/api/edge-requests", payload)
        except HTTPError as exc:
            assert exc.code == 400
            assert json.load(exc)["error"] == "invalid_edge_request"
        else:
            raise AssertionError("Unknown fields should be rejected")
        assert (
            "sentinel_runtime_rejection_audit_error error_type=RuntimeError"
            in caplog.text
        )
        assert "Simulated audit failure" not in caplog.text
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        registry.close()

def test_rejected_http_request_is_persisted_safely_in_sqlite(tmp_path):
    registry = make_registry()
    runtime = LocalSentinelRuntime(registry, batch_size=1)
    audit = PersistentAuditTrail(tmp_path / "audit.sqlite3")

    def audit_rejection(status, error_code):
        audit.record({
            "event_type": "HTTP_REQUEST_REJECTED",
            "http_status": status,
            "error_code": error_code,
        })

    server = create_runtime_server(
        runtime, port=0, rejection_callback=audit_rejection
    )
    thread = threading.Thread(
        target=lambda: server.serve_forever(poll_interval=0.01),
        daemon=True,
    )
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"

    try:
        payload = {
            "request_id": "req-audit-test",
            "trace_id": "trace-audit-test",
            "edge_id": "edge",
            "source_runtime_id": "runtime-A-001",
            "target_runtime_id": "runtime-B-001",
            "action": "SEND_MESSAGE",
            "requested_permission": "message:send",
            "credentials_reference": "SECRET_CREDENTIAL",
            "raw_body": "SECRET_REQUEST_BODY",
        }

        try:
            post_json(base + "/api/edge-requests", payload)
        except HTTPError as exc:
            assert exc.code == 400
            exc.close()
        else:
            raise AssertionError("Unknown fields should be rejected")

        records = audit.list_recent()
        assert len(records) == 1
        assert records[0]["event_type"] == "HTTP_REQUEST_REJECTED"
        assert records[0]["http_status"] == 400
        assert records[0]["error_code"] == "invalid_edge_request"
        assert "SECRET_CREDENTIAL" not in repr(records)
        assert "SECRET_REQUEST_BODY" not in repr(records)
        assert audit.verify_integrity()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        audit.close()
        registry.close()
