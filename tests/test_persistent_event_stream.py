import sqlite3

import pytest

from sentinelagent.event_stream import EventStream
from sentinelagent.micro_batch import MicroBatchProcessor
from sentinelagent.persistent_event_stream import PersistentEventStream


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
