from __future__ import annotations

import sqlite3

import pytest

from sentinelagent.persistent_audit import PersistentAuditTrail


def test_records_persist_and_are_returned_newest_first(tmp_path):
    path = tmp_path / "audit.sqlite3"
    trail = PersistentAuditTrail(path)
    first = trail.record({"event_type": "EDGE_AUTHORIZATION", "decision": "ALLOW"})
    second = trail.record({"event_type": "EDGE_AUTHORIZATION", "decision": "DENY"})
    assert first["audit_id"] < second["audit_id"]
    assert trail.verify_integrity()
    assert [event["decision"] for event in trail.list_recent()] == ["DENY", "ALLOW"]
    trail.close()

    reopened = PersistentAuditTrail(path)
    assert reopened.verify_integrity()
    assert len(reopened.list_recent()) == 2
    reopened.close()


def test_chain_detects_modified_record(tmp_path):
    path = tmp_path / "audit.sqlite3"
    trail = PersistentAuditTrail(path)
    trail.record({"event_type": "EDGE_AUTHORIZATION", "decision": "DENY"})
    with trail._lock:
        trail._connection.execute(
            "UPDATE audit_entries SET event_json = ? WHERE audit_id = 1",
            ('{"captured_at":"fake","decision":"ALLOW","event_type":"EDGE_AUTHORIZATION"}',),
        )
    assert trail.verify_integrity() is False
    trail.close()
    with pytest.raises(ValueError, match="integrity"):
        PersistentAuditTrail(path)


def test_invalid_events_and_limits_are_rejected(tmp_path):
    trail = PersistentAuditTrail(tmp_path / "audit.sqlite3")
    with pytest.raises(ValueError, match="event_type"):
        trail.record({"decision": "DENY"})
    with pytest.raises(ValueError, match="limit"):
        trail.list_recent(0)
    with pytest.raises(TypeError, match="dictionary"):
        trail.record("not-an-event")  # type: ignore[arg-type]
    trail.close()


def test_write_uses_hash_chain(tmp_path):
    trail = PersistentAuditTrail(tmp_path / "audit.sqlite3")
    first = trail.record({"event_type": "ONE"})
    second = trail.record({"event_type": "TWO"})
    assert second["previous_hash"] == first["event_hash"]
    assert len(second["event_hash"]) == 64
    assert trail.verify_integrity()
    trail.close()
