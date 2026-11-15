from __future__ import annotations

import json
import sys
import pytest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from scripts.run_live_runtime import seed_missing_agents  # noqa: E402
from scripts.run_audited_runtime import AuditedLocalSentinelRuntime  # noqa: E402
from sentinelagent.persistent_audit import PersistentAuditTrail  # noqa: E402
from sentinelagent.registry import AgentRegistry  # noqa: E402


def test_allowed_and_denied_requests_appear_in_durable_audit(tmp_path):
    registry = AgentRegistry(database_path=tmp_path / "registry.sqlite3")
    audit = PersistentAuditTrail(tmp_path / "audit.sqlite3")
    seed_missing_agents(registry, ROOT / "config" / "local-agents.example.json")
    runtime = AuditedLocalSentinelRuntime(registry, audit, batch_size=10, max_wait_seconds=1.0)
    try:
        base = {
            "trace_id": "trace-test",
            "edge_id": "edge-agent-A-agent-B",
            "source_runtime_id": "runtime-A-001",
            "target_runtime_id": "runtime-B-001",
            "resource_id": "resource-test",
        }
        allowed = runtime.submit_edge_request({
            **base,
            "request_id": "audit-allow",
            "action": "SEND_MESSAGE",
            "requested_permission": "message:send",
        })
        denied = runtime.submit_edge_request({
            **base,
            "request_id": "audit-deny",
            "action": "DELETE_RECORD",
            "requested_permission": "admin:delete",
        })
        assert allowed["decision"] == "ALLOW"
        assert denied["decision"] == "DENY"
        records = audit.list_recent()
        assert [row["request_id"] for row in records[:2]] == ["audit-deny", "audit-allow"]
        assert records[0]["decision"] == "DENY"
        assert records[1]["decision"] == "ALLOW"
        assert runtime.dashboard_provider()["audit_trail_configured"] is True
        assert runtime.dashboard_provider()["audit_trail"][0]["request_id"] == "audit-deny"
        assert audit.verify_integrity()
    finally:
        audit.close()
        registry.close()


def test_audit_excludes_arbitrary_payload_fields(tmp_path):
    registry = AgentRegistry(database_path=tmp_path / "registry.sqlite3")
    audit = PersistentAuditTrail(tmp_path / "audit.sqlite3")
    seed_missing_agents(registry, ROOT / "config" / "local-agents.example.json")
    runtime = AuditedLocalSentinelRuntime(registry, audit)
    try:
        payload = {
            "request_id": "audit-allowlist",
            "trace_id": "trace-test",
            "edge_id": "edge-agent-A-agent-B",
            "source_runtime_id": "runtime-A-001",
            "target_runtime_id": "runtime-B-001",
            "action": "SEND_MESSAGE",
            "requested_permission": "message:send",
            "resource_id": "resource-test",
            "secret": "do-not-record-this",
        }
        try:
            runtime.submit_edge_request(payload)
        except ValueError:
            # The runtime rejects extra keys; it should not audit the arbitrary
            # input value as part of a successful authorization record.
            pass
        assert all("secret" not in record for record in audit.list_recent())
        assert all("do-not-record-this" not in json.dumps(record) for record in audit.list_recent())
    finally:
        audit.close()
        registry.close()


def test_authorization_fails_closed_when_audit_write_fails(tmp_path, monkeypatch):
    registry = AgentRegistry(database_path=tmp_path / "registry.sqlite3")
    audit = PersistentAuditTrail(tmp_path / "audit.sqlite3")
    seed_missing_agents(registry, ROOT / "config" / "local-agents.example.json")
    runtime = AuditedLocalSentinelRuntime(registry, audit)

    def fail_record_in_transaction(connection, event):
        raise OSError("simulated audit database failure")

    monkeypatch.setattr(
    audit,
    "record_in_transaction",
    fail_record_in_transaction,
)

    payload = {
        "trace_id": "trace-audit-failure",
        "edge_id": "edge-agent-A-agent-B",
        "source_runtime_id": "runtime-A-001",
        "target_runtime_id": "runtime-B-001",
        "resource_id": "resource-test",
        "request_id": "audit-write-failure",
        "action": "SEND_MESSAGE",
        "requested_permission": "message:send",
    }

    try:
        with pytest.raises(OSError, match="simulated audit database failure"):
            runtime.submit_edge_request(payload)
    finally:
        audit.close()
        registry.close()


def test_error_audit_failure_does_not_hide_original_authorization_error(
    tmp_path, monkeypatch
):
    registry = AgentRegistry(database_path=tmp_path / "registry.sqlite3")
    audit = PersistentAuditTrail(tmp_path / "audit.sqlite3")
    seed_missing_agents(registry, ROOT / "config" / "local-agents.example.json")
    runtime = AuditedLocalSentinelRuntime(registry, audit)

    def fail_record(event):
        raise OSError("simulated audit database failure")

    monkeypatch.setattr(audit, "record", fail_record)

    try:
        with pytest.raises(ValueError, match="unknown"):
            runtime.submit_edge_request({"unknown_field": "invalid"})
    finally:
        audit.close()
        registry.close()