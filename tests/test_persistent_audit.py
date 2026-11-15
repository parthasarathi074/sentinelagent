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


def test_insert_failure_rolls_back_transaction(tmp_path):
    path = tmp_path / "audit.sqlite3"
    trail = PersistentAuditTrail(path)

    trail._connection.execute(
        """
        CREATE TRIGGER fail_audit_insert
        BEFORE INSERT ON audit_entries
        BEGIN
            SELECT RAISE(ABORT, 'simulated insert failure');
        END;
        """
    )

    with pytest.raises(sqlite3.IntegrityError, match="simulated insert failure"):
        trail.record({"event_type": "TEST_FAILURE"})

    assert trail._connection.in_transaction is False
    assert trail.list_recent() == []

    trail._connection.execute("DROP TRIGGER fail_audit_insert")
    record = trail.record({"event_type": "TEST_RECOVERY"})
    assert record["event_type"] == "TEST_RECOVERY"
    assert trail.verify_integrity()

    trail.close()


def test_rollback_failure_does_not_hide_original_insert_error(tmp_path):
    trail = PersistentAuditTrail(tmp_path / "audit.sqlite3")

    trail._connection.execute(
        """
        CREATE TRIGGER fail_audit_insert
        BEFORE INSERT ON audit_entries
        BEGIN
            SELECT RAISE(ABORT, 'original insert failure');
        END;
        """
    )

    original_connection = trail._connection

    class RollbackFailingConnection:
        def execute(self, sql, parameters=()):
            if sql == "ROLLBACK":
                raise sqlite3.OperationalError("simulated rollback failure")
            return original_connection.execute(sql, parameters)

    trail._connection = RollbackFailingConnection()

    with pytest.raises(sqlite3.IntegrityError, match="original insert failure"):
        trail.record({"event_type": "TEST_FAILURE"})

    original_connection.close()


def test_record_in_transaction_respects_caller_rollback(tmp_path):
    trail = PersistentAuditTrail(tmp_path / "audit.sqlite3")
    connection = trail._connection

    connection.execute("BEGIN IMMEDIATE")

    record = trail.record_in_transaction(
        connection,
        {
            "event_type": "TRANSACTIONAL_TEST",
            "decision": "ALLOW",
        },
    )

    assert record["event_type"] == "TRANSACTIONAL_TEST"
    assert connection.in_transaction is True
    assert len(trail.list_recent()) == 1

    connection.execute("ROLLBACK")

    assert connection.in_transaction is False
    assert trail.list_recent() == []
    assert trail.verify_integrity() is True

    trail.close()
