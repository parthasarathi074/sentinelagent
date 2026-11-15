import sqlite3

import pytest

from sentinelagent.event_stream import EventStream
from sentinelagent.micro_batch import MicroBatchProcessor
from sentinelagent.persistent_event_stream import PersistentEventStream
from sentinelagent.persistent_audit import PersistentAuditTrail


def test_events_survive_close_and_reopen(tmp_path):
    database_path = tmp_path / "events.db"

    stream = PersistentEventStream(database_path)
    stream.publish({"agent": "A", "event": "ingested"})
    stream.close()

    reopened = PersistentEventStream(database_path)
    try:
        assert reopened.size() == 1
        assert reopened.consume_batch(1) == [
            {"agent": "A", "event": "ingested"}
        ]
    finally:
        reopened.close()


def test_events_are_consumed_in_publication_order(tmp_path):
    stream = PersistentEventStream(tmp_path / "events.db")
    try:
        stream.publish({"sequence": 1})
        stream.publish({"sequence": 2})
        stream.publish({"sequence": 3})

        assert stream.consume_batch(2) == [
            {"sequence": 1},
            {"sequence": 2},
        ]
        assert stream.consume_batch(2) == [{"sequence": 3}]
    finally:
        stream.close()


def test_interrupted_batch_returns_to_pending_after_restart(tmp_path):
    database_path = tmp_path / "events.db"

    stream = PersistentEventStream(database_path)
    stream.publish({"agent": "A"})
    assert stream.consume_batch(1) == [{"agent": "A"}]
    stream.close()

    reopened = PersistentEventStream(database_path)
    try:
        assert reopened.size() == 1
        assert reopened.consume_batch(1) == [{"agent": "A"}]
    finally:
        reopened.close()


def test_acknowledged_events_remain_in_audit_history(tmp_path):
    stream = PersistentEventStream(tmp_path / "events.db")
    try:
        stream.publish({"agent": "A"})
        batch = stream.consume_batch(1)
        stream.acknowledge_batch(batch)

        assert stream.size() == 0

        completed_count = stream._connection.execute(
            "SELECT COUNT(*) FROM events WHERE status = 'completed'"
        ).fetchone()[0]
        assert completed_count == 1

        acknowledged_count = stream._connection.execute(
            "SELECT COUNT(*) FROM batches WHERE status = 'acknowledged'"
        ).fetchone()[0]
        assert acknowledged_count == 1

        linked_count = stream._connection.execute(
            "SELECT COUNT(*) FROM batch_events"
        ).fetchone()[0]
        assert linked_count == 1
    finally:
        stream.close()


def test_invalid_batch_sizes_are_rejected():
    stream = PersistentEventStream(":memory:")
    try:
        for invalid_size in (0, -1, 1.5, "2", None, True):
            with pytest.raises(ValueError):
                stream.consume_batch(invalid_size)
    finally:
        stream.close()


def test_non_json_serializable_event_is_rejected():
    stream = PersistentEventStream(":memory:")
    try:
        with pytest.raises(TypeError):
            stream.publish({"not_serializable": object()})

        assert stream.size() == 0
    finally:
        stream.close()


def test_non_finite_numbers_are_rejected():
    stream = PersistentEventStream(":memory:")
    try:
        with pytest.raises(ValueError):
            stream.publish({"value": float("nan")})

        assert stream.size() == 0
    finally:
        stream.close()


def test_micro_batch_acknowledges_successful_sqlite_batch(tmp_path):
    stream = PersistentEventStream(tmp_path / "events.db")
    try:
        stream.publish({"agent": "A"})
        processor = MicroBatchProcessor(
            event_stream=stream,
            batch_size=1,
            batch_handler=lambda batch: None,
        )

        assert processor.process_once() == [{"agent": "A"}]
        assert stream.size() == 0

        completed_count = stream._connection.execute(
            "SELECT COUNT(*) FROM events WHERE status = 'completed'"
        ).fetchone()[0]
        assert completed_count == 1
    finally:
        stream.close()


def test_micro_batch_restores_failed_sqlite_batch(tmp_path):
    stream = PersistentEventStream(tmp_path / "events.db")
    try:
        stream.publish({"agent": "A"})

        def failing_handler(batch):
            raise RuntimeError("simulated processing failure")

        processor = MicroBatchProcessor(
            event_stream=stream,
            batch_size=1,
            batch_handler=failing_handler,
        )

        with pytest.raises(RuntimeError, match="simulated processing failure"):
            processor.process_once()

        assert stream.size() == 1

        retry_batch = stream.consume_batch(1)
        assert retry_batch == [{"agent": "A"}]
        stream.restore_batch(retry_batch)
    finally:
        stream.close()


def test_batch_id_is_preserved_after_restart_recovery(tmp_path):
    database_path = tmp_path / "events.db"
    stream = PersistentEventStream(database_path)
    stream.publish({"agent": "A"})

    batch = stream.consume_batch(1)
    original_batch_id = stream.batch_id_for(batch)
    stream.close()

    recovered_stream = PersistentEventStream(database_path)
    try:
        recovered_batch = recovered_stream.consume_batch(1)

        assert recovered_batch == [{"agent": "A"}]
        assert recovered_stream.batch_id_for(recovered_batch) == original_batch_id
        recovered_stream.acknowledge_batch(recovered_batch)
    finally:
        recovered_stream.close()


def test_retry_keeps_original_batch_separate_from_new_events(tmp_path):
    stream = PersistentEventStream(tmp_path / "events.db")

    try:
        stream.publish({"agent": "A"})
        failed_batch = stream.consume_batch(1)
        original_batch_id = stream.batch_id_for(failed_batch)

        stream.restore_batch(failed_batch)
        stream.publish({"agent": "B"})

        retry_batch = stream.consume_batch(2)

        assert retry_batch == [{"agent": "A"}]
        assert stream.batch_id_for(retry_batch) == original_batch_id

        stream.acknowledge_batch(retry_batch)

        next_batch = stream.consume_batch(2)
        assert next_batch == [{"agent": "B"}]
        assert stream.batch_id_for(next_batch) != original_batch_id
    finally:
        stream.close()


def test_micro_batch_still_works_with_in_memory_event_stream():
    stream = EventStream()
    stream.publish({"agent": "A"})
    processed = []

    processor = MicroBatchProcessor(
        event_stream=stream,
        batch_size=1,
        batch_handler=lambda batch: processed.extend(batch),
    )

    assert processor.process_once() == [{"agent": "A"}]
    assert processed == [{"agent": "A"}]
    assert stream.size() == 0


def test_acknowledgement_failure_is_reported(tmp_path):
    stream = PersistentEventStream(tmp_path / "events.db")
    handler_calls = []

    try:
        stream.publish({"agent": "A"})

        def handler(batch):
            handler_calls.append(batch)

        def fail_acknowledgement(batch):
            raise RuntimeError("simulated acknowledgement failure")

        stream.acknowledge_batch = fail_acknowledgement

        processor = MicroBatchProcessor(
            event_stream=stream,
            batch_size=1,
            batch_handler=handler,
        )

        with pytest.raises(
            RuntimeError,
            match="simulated acknowledgement failure",
        ):
            processor.process_once()

        assert handler_calls == [[{"agent": "A"}]]
        assert stream.size() == 0
        assert stream._connection.execute(
            "SELECT status FROM events"
        ).fetchone()[0] == "in_flight"
    finally:
        stream.close()


def test_partial_retry_survives_restart_without_losing_events(tmp_path):
    database_path = tmp_path / "events.db"
    stream = PersistentEventStream(database_path)
    try:
        stream.publish({"agent": "A"})
        stream.publish({"agent": "B"})
        original_batch = stream.consume_batch(2)
        original_batch_id = stream.batch_id_for(original_batch)
        stream.restore_batch(original_batch)
        retry_a = stream.consume_batch(1)
        assert retry_a == [{"agent": "A"}]
        assert stream.batch_id_for(retry_a) == original_batch_id
        # Simulate a crash before acknowledging the partial retry.
        stream.close()
        recovered_stream = PersistentEventStream(database_path)
        try:
            assert recovered_stream.size() == 2
            recovered_batch = recovered_stream.consume_batch(1)
            assert recovered_batch == [{"agent": "A"}]
            assert (
                recovered_stream.batch_id_for(recovered_batch)
                == original_batch_id
            )
            recovered_stream.acknowledge_batch(recovered_batch)
            remaining_batch = recovered_stream.consume_batch(1)
            assert remaining_batch == [{"agent": "B"}]
            recovered_stream.acknowledge_batch(remaining_batch)
            assert recovered_stream.size() == 0
        finally:
            recovered_stream.close()
    finally:
        if stream._connection is not None:
            stream.close()


def test_incompatible_legacy_schema_is_rejected_without_data_loss(tmp_path):
    database_path = tmp_path / "legacy.db"

    # Simulate an older database whose events table does not allow
    # the "completed" status. Include an in-flight batch and its link.
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TABLE events (
                sequence_number INTEGER PRIMARY KEY AUTOINCREMENT,
                event_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'in_flight'))
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE batches (
                batch_id TEXT PRIMARY KEY,
                status TEXT NOT NULL
                    CHECK (status IN ('in_flight', 'acknowledged'))
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE batch_events (
                batch_id TEXT NOT NULL,
                sequence_number INTEGER NOT NULL UNIQUE,
                PRIMARY KEY (batch_id, sequence_number),
                FOREIGN KEY (batch_id) REFERENCES batches(batch_id),
                FOREIGN KEY (sequence_number)
                    REFERENCES events(sequence_number)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO events (sequence_number, event_json, created_at, status)
            VALUES (1, '{"agent": "A"}', 123.0, 'in_flight')
            """
        )
        connection.execute(
            """
            INSERT INTO batches (batch_id, status)
            VALUES ('batch-1', 'in_flight')
            """
        )
        connection.execute(
            """
            INSERT INTO batch_events (batch_id, sequence_number)
            VALUES ('batch-1', 1)
            """
        )

    def read_database_state():
        with sqlite3.connect(database_path) as connection:
            tables = connection.execute(
                """
                SELECT name, sql
                FROM sqlite_master
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
            ).fetchall()
            events = connection.execute(
                """
                SELECT sequence_number, event_json, created_at, status
                FROM events ORDER BY sequence_number
                """
            ).fetchall()
            batches = connection.execute(
                "SELECT batch_id, status FROM batches ORDER BY batch_id"
            ).fetchall()
            links = connection.execute(
                """
                SELECT batch_id, sequence_number
                FROM batch_events ORDER BY batch_id, sequence_number
                """
            ).fetchall()

        return tables, events, batches, links

    before = read_database_state()

    with pytest.raises(RuntimeError, match="incompatible database schema"):
        PersistentEventStream(database_path)

    after = read_database_state()

    assert after == before
    assert after[1] == [(1, '{"agent": "A"}', 123.0, "in_flight")]
    assert after[2] == [("batch-1", "in_flight")]
    assert after[3] == [("batch-1", 1)]


def test_restored_batches_keep_separate_ids_when_retried_together(tmp_path):
    stream = PersistentEventStream(tmp_path / "events.db")
    try:
        stream.publish({"agent": "A"})
        stream.publish({"agent": "B"})
        batch_a = stream.consume_batch(1)
        batch_a_id = stream.batch_id_for(batch_a)
        batch_b = stream.consume_batch(1)
        batch_b_id = stream.batch_id_for(batch_b)
        assert batch_a_id != batch_b_id
        stream.restore_batch(batch_a)
        stream.restore_batch(batch_b)
        retry_a = stream.consume_batch(2)
        # A retry must not merge two separately identified batches.
        assert retry_a == [{"agent": "A"}]
        assert stream.batch_id_for(retry_a) == batch_a_id
        stream.acknowledge_batch(retry_a)
        retry_b = stream.consume_batch(2)
        assert retry_b == [{"agent": "B"}]
        assert stream.batch_id_for(retry_b) == batch_b_id
        stream.acknowledge_batch(retry_b)
    finally:
        stream.close()


def test_partial_retry_does_not_complete_unreturned_events(tmp_path):
    stream = PersistentEventStream(tmp_path / "events.db")
    try:
        stream.publish({"agent": "A"})
        stream.publish({"agent": "B"})
        original_batch = stream.consume_batch(2)
        original_batch_id = stream.batch_id_for(original_batch)
        stream.restore_batch(original_batch)
        retry_a = stream.consume_batch(1)
        assert retry_a == [{"agent": "A"}]
        assert stream.batch_id_for(retry_a) == original_batch_id
        stream.acknowledge_batch(retry_a)
        # Event B was not returned or acknowledged.
        assert stream.size() == 1
        retry_b = stream.consume_batch(1)
        assert retry_b == [{"agent": "B"}]
        assert stream.batch_id_for(retry_b) != original_batch_id
        stream.acknowledge_batch(retry_b)
        assert stream.size() == 0
    finally:
        stream.close()


def test_event_and_audit_are_rolled_back_together_when_audit_fails(
    tmp_path,
):
    database_path = tmp_path / "shared.db"

    audit = PersistentAuditTrail(database_path)
    stream = PersistentEventStream(
        database_path,
        audit_trail=audit,
    )

    try:
        original_record = audit.record_in_transaction

        def failing_audit_write(connection, event):
            original_record(connection, event)
            raise RuntimeError("simulated audit failure")

        audit.record_in_transaction = failing_audit_write

        with pytest.raises(RuntimeError, match="simulated audit failure"):
            stream.publish(
                {"agent": "A", "event": "accepted"},
                audit_event={
                    "event_type": "EDGE_AUTHORIZATION",
                    "decision": "allowed",
                },
            )

        event_count = stream._connection.execute(
            "SELECT COUNT(*) FROM events"
        ).fetchone()[0]

        audit_count = stream._connection.execute(
            "SELECT COUNT(*) FROM audit_entries"
        ).fetchone()[0]

        assert event_count == 0
        assert audit_count == 0

    finally:
        stream.close()
        audit.close()


def test_event_and_audit_are_saved_together_when_publish_succeeds(
    tmp_path,
):
    database_path = tmp_path / "shared_success.db"

    audit = PersistentAuditTrail(database_path)
    stream = PersistentEventStream(
        database_path,
        audit_trail=audit,
    )

    try:
        stream.publish(
            {"agent": "A", "event": "accepted"},
            audit_event={
                "event_type": "EDGE_AUTHORIZATION",
                "decision": "allowed",
            },
        )

        event_count = stream._connection.execute(
            "SELECT COUNT(*) FROM events"
        ).fetchone()[0]

        audit_count = stream._connection.execute(
            "SELECT COUNT(*) FROM audit_entries"
        ).fetchone()[0]

        assert event_count == 1
        assert audit_count == 1
        assert stream.size() == 1

    finally:
        stream.close()
        audit.close()



def test_batch_lifecycle_is_audited_in_order(tmp_path):
    from sentinelagent.persistent_audit import PersistentAuditTrail
    from sentinelagent.persistent_event_stream import PersistentEventStream

    database = tmp_path / "lifecycle.sqlite3"
    audit = PersistentAuditTrail(database)
    stream = PersistentEventStream(database, audit_trail=audit)

    try:
        stream.publish({"event": "test"})
        batch = stream.consume_batch(1)
        stream.acknowledge_batch(batch)

        records = list(reversed(audit.list_recent()))
        event_types = [record["event_type"] for record in records]

        assert event_types == [
            "BATCH_PROCESSING_STARTED",
            "BATCH_PROCESSING_SUCCEEDED",
        ]
        assert records[0]["batch_id"] == records[1]["batch_id"]
        assert records[0]["event_count"] == 1
        assert records[1]["event_count"] == 1
        assert audit.verify_integrity()
    finally:
        stream.close()
        audit.close()


def test_failed_batch_records_error_type_and_can_be_retried(tmp_path):
    from sentinelagent.persistent_audit import PersistentAuditTrail
    from sentinelagent.persistent_event_stream import PersistentEventStream

    database = tmp_path / "failed-lifecycle.sqlite3"
    audit = PersistentAuditTrail(database)
    stream = PersistentEventStream(database, audit_trail=audit)

    try:
        stream.publish({"event": "test"})
        batch = stream.consume_batch(1)
        original_batch_id = stream.batch_id_for(batch)

        stream.restore_batch(batch, error_type="RuntimeError")

        retry = stream.consume_batch(1)

        assert stream.batch_id_for(retry) == original_batch_id

        records = list(reversed(audit.list_recent()))
        event_types = [record["event_type"] for record in records]

        assert event_types == [
            "BATCH_PROCESSING_STARTED",
            "BATCH_PROCESSING_FAILED",
            "BATCH_PROCESSING_RETRIED",
        ]
        assert records[1]["error_type"] == "RuntimeError"
        assert audit.verify_integrity()
    finally:
        stream.close()
        audit.close()


def test_restart_recovery_is_audited_and_preserves_batch_id(tmp_path):
    from sentinelagent.persistent_audit import PersistentAuditTrail
    from sentinelagent.persistent_event_stream import PersistentEventStream

    database = tmp_path / "recovery-lifecycle.sqlite3"
    audit = PersistentAuditTrail(database)
    first_stream = PersistentEventStream(database, audit_trail=audit)

    first_stream.publish({"event": "test"})
    first_batch = first_stream.consume_batch(1)
    original_batch_id = first_stream.batch_id_for(first_batch)
    first_stream.close()

    recovered_stream = PersistentEventStream(
        database,
        audit_trail=audit,
    )

    try:
        retry = recovered_stream.consume_batch(1)

        assert recovered_stream.batch_id_for(retry) == original_batch_id

        records = list(reversed(audit.list_recent()))
        event_types = [record["event_type"] for record in records]

        assert event_types == [
            "BATCH_PROCESSING_STARTED",
            "BATCH_RECOVERED",
            "BATCH_PROCESSING_RETRIED",
        ]
        assert audit.verify_integrity()
    finally:
        recovered_stream.close()
        audit.close()


def test_lifecycle_audit_failure_rolls_back_batch_claim(tmp_path):
    from sentinelagent.persistent_audit import PersistentAuditTrail
    from sentinelagent.persistent_event_stream import PersistentEventStream

    database = tmp_path / "claim-rollback.sqlite3"
    audit = PersistentAuditTrail(database)
    stream = PersistentEventStream(database, audit_trail=audit)

    try:
        stream.publish({"event": "test"})

        original_record = audit.record_in_transaction

        def failing_record(connection, event):
            raise RuntimeError("simulated lifecycle audit failure")

        audit.record_in_transaction = failing_record

        try:
            stream.consume_batch(1)
        except RuntimeError as exc:
            assert str(exc) == "simulated lifecycle audit failure"
        else:
            raise AssertionError("Expected lifecycle audit failure")

        audit.record_in_transaction = original_record

        assert stream.size() == 1

        batch = stream.consume_batch(1)
        assert len(batch) == 1
        assert audit.verify_integrity()
    finally:
        stream.close()
        audit.close()


def test_success_audit_failure_rolls_back_acknowledgement(tmp_path):
    from sentinelagent.persistent_audit import PersistentAuditTrail
    from sentinelagent.persistent_event_stream import PersistentEventStream

    database = tmp_path / "ack-rollback.sqlite3"
    audit = PersistentAuditTrail(database)
    stream = PersistentEventStream(database, audit_trail=audit)

    try:
        stream.publish({"event": "test"})
        batch = stream.consume_batch(1)

        original_record = audit.record_in_transaction

        def failing_record(connection, event):
            raise RuntimeError("simulated success audit failure")

        audit.record_in_transaction = failing_record

        try:
            stream.acknowledge_batch(batch)
        except RuntimeError as exc:
            assert str(exc) == "simulated success audit failure"
        else:
            raise AssertionError("Expected lifecycle audit failure")

        audit.record_in_transaction = original_record

        # The failed acknowledgement must leave the batch in flight.
        assert stream.size() == 0

        stream.restore_batch(batch)
        assert stream.size() == 1
        assert audit.verify_integrity()
    finally:
        stream.close()
        audit.close()


def test_recovery_audit_failure_rolls_back_recovery(tmp_path):
    from sentinelagent.persistent_audit import PersistentAuditTrail
    from sentinelagent.persistent_event_stream import PersistentEventStream

    database = tmp_path / "recovery-rollback.sqlite3"
    audit = PersistentAuditTrail(database)
    first_stream = PersistentEventStream(database, audit_trail=audit)

    first_stream.publish({"event": "test"})
    first_stream.consume_batch(1)
    first_stream.close()

    original_record = audit.record_in_transaction

    def failing_record(connection, event):
        raise RuntimeError("simulated recovery audit failure")

    audit.record_in_transaction = failing_record

    try:
        try:
            PersistentEventStream(database, audit_trail=audit)
        except RuntimeError as exc:
            assert str(exc) == "simulated recovery audit failure"
        else:
            raise AssertionError("Expected recovery audit failure")

        audit.record_in_transaction = original_record

        # A second startup must still be able to recover the batch.
        recovered_stream = PersistentEventStream(
            database,
            audit_trail=audit,
        )
        try:
            batch = recovered_stream.consume_batch(1)
            assert len(batch) == 1

            records = list(reversed(audit.list_recent()))
            event_types = [record["event_type"] for record in records]

            assert "BATCH_RECOVERED" in event_types
            assert audit.verify_integrity()
        finally:
            recovered_stream.close()
    finally:
        audit.record_in_transaction = original_record
        audit.close()
