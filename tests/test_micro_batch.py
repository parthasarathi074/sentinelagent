import unittest

from sentinelagent.event_stream import EventStream
from sentinelagent.micro_batch import MicroBatchProcessor


class FakeClock:
    def __init__(self):
        self.current = 0.0

    def __call__(self):
        return self.current

    def advance(self, seconds):
        self.current += seconds


class TestMicroBatchProcessor(unittest.TestCase):

    def test_processes_batch_when_batch_size_is_reached(self):
        stream = EventStream()
        processed_batches = []

        processor = MicroBatchProcessor(
            event_stream=stream,
            batch_size=3,
            batch_handler=processed_batches.append,
        )

        stream.publish({"event_id": "event-1"})
        stream.publish({"event_id": "event-2"})
        stream.publish({"event_id": "event-3"})

        result = processor.process_once()

        self.assertEqual(
            result,
            [
                {"event_id": "event-1"},
                {"event_id": "event-2"},
                {"event_id": "event-3"},
            ],
        )
        self.assertEqual(len(processed_batches), 1)

    def test_successful_in_memory_batch_releases_restore_metadata(self):
        stream = EventStream()
        event = {"event_id": "event-1"}
        stream.publish(event)

        processor = MicroBatchProcessor(
            event_stream=stream,
            batch_size=1,
            batch_handler=lambda batch: None,
        )

        self.assertEqual(processor.process_once(), [event])
        self.assertEqual(stream._consumed_batches, {})

    def test_waits_when_batch_is_not_full(self):
        stream = EventStream()
        processed_batches = []

        processor = MicroBatchProcessor(
            event_stream=stream,
            batch_size=3,
            batch_handler=processed_batches.append,
        )

        stream.publish({"event_id": "event-1"})
        stream.publish({"event_id": "event-2"})

        result = processor.process_once()

        self.assertEqual(result, [])
        self.assertEqual(processed_batches, [])
        self.assertEqual(stream.size(), 2)

    def test_remaining_events_stay_in_stream(self):
        stream = EventStream()
        processed_batches = []

        processor = MicroBatchProcessor(
            event_stream=stream,
            batch_size=3,
            batch_handler=processed_batches.append,
        )

        for event_id in range(1, 6):
            stream.publish({"event_id": f"event-{event_id}"})

        result = processor.process_once()

        self.assertEqual(len(result), 3)
        self.assertEqual(stream.size(), 2)

    def test_invalid_batch_size(self):
        stream = EventStream()

        with self.assertRaises(ValueError):
            MicroBatchProcessor(
                event_stream=stream,
                batch_size=0,
                batch_handler=lambda batch: None,
            )

    def test_batch_size_must_be_a_positive_integer(self):
        stream = EventStream()
        for invalid_size in (True, 1.5, "2", None):
            with self.subTest(batch_size=invalid_size):
                with self.assertRaises(ValueError):
                    MicroBatchProcessor(
                        event_stream=stream,
                        batch_size=invalid_size,
                        batch_handler=lambda batch: None,
                    )

    def test_max_wait_must_be_finite_and_positive(self):
        stream = EventStream()
        for invalid_wait in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(max_wait_seconds=invalid_wait):
                with self.assertRaises(ValueError):
                    MicroBatchProcessor(
                        event_stream=stream,
                        batch_size=1,
                        max_wait_seconds=invalid_wait,
                        batch_handler=lambda batch: None,
                    )

    def test_partial_batch_waits_until_max_wait(self):
        clock = FakeClock()
        stream = EventStream(clock=clock)
        processed_batches = []

        processor = MicroBatchProcessor(
            event_stream=stream,
            batch_size=3,
            max_wait_seconds=10,
            batch_handler=processed_batches.append,
            clock=clock,
        )

        stream.publish({"event_id": "event-1"})
        stream.publish({"event_id": "event-2"})

        result = processor.process_once()

        self.assertEqual(result, [])
        self.assertEqual(processed_batches, [])

        clock.advance(9)
        result = processor.process_once()

        self.assertEqual(result, [])
        self.assertEqual(processed_batches, [])

    def test_partial_batch_processes_after_max_wait(self):
        clock = FakeClock()
        stream = EventStream(clock=clock)
        processed_batches = []

        processor = MicroBatchProcessor(
            event_stream=stream,
            batch_size=3,
            max_wait_seconds=10,
            batch_handler=processed_batches.append,
            clock=clock,
        )

        stream.publish({"event_id": "event-1"})
        stream.publish({"event_id": "event-2"})

        processor.process_once()
        clock.advance(10)

        result = processor.process_once()

        self.assertEqual(
            result,
            [
                {"event_id": "event-1"},
                {"event_id": "event-2"},
            ],
        )
        self.assertEqual(len(processed_batches), 1)

    def test_invalid_max_wait_is_rejected(self):
        stream = EventStream()

        with self.assertRaises(ValueError):
            MicroBatchProcessor(
                event_stream=stream,
                batch_size=3,
                max_wait_seconds=0,
                batch_handler=lambda batch: None,
            )

    def test_remaining_events_start_a_new_wait_window(self):
        clock = FakeClock()
        stream = EventStream(clock=clock)
        processed_batches = []

        processor = MicroBatchProcessor(
            event_stream=stream,
            batch_size=3,
            max_wait_seconds=10,
            batch_handler=processed_batches.append,
            clock=clock,
        )

        for event_id in range(1, 6):
            stream.publish({"event_id": f"event-{event_id}"})

        result = processor.process_once()

        self.assertEqual(
            result,
            [
                {"event_id": "event-1"},
                {"event_id": "event-2"},
                {"event_id": "event-3"},
            ],
        )
        self.assertEqual(stream.size(), 2)

        result = processor.process_once()
        self.assertEqual(result, [])

        clock.advance(9)
        result = processor.process_once()
        self.assertEqual(result, [])

        clock.advance(1)
        result = processor.process_once()

        self.assertEqual(
            result,
            [
                {"event_id": "event-4"},
                {"event_id": "event-5"},
            ],
        )
        self.assertEqual(stream.size(), 0)

    def test_failed_handler_restores_batch_for_retry(self):
        stream = EventStream()
        attempts = []

        def handler(batch):
            attempts.append(batch)
            if len(attempts) == 1:
                raise RuntimeError("simulated analysis failure")

        processor = MicroBatchProcessor(
            event_stream=stream,
            batch_size=2,
            batch_handler=handler,
        )

        stream.publish({"event_id": "event-1"})
        stream.publish({"event_id": "event-2"})

        with self.assertRaisesRegex(
            RuntimeError,
            "simulated analysis failure",
        ):
            processor.process_once()

        self.assertEqual(stream.size(), 2)

        result = processor.process_once()

        self.assertEqual(
            result,
            [
                {"event_id": "event-1"},
                {"event_id": "event-2"},
            ],
        )
        self.assertEqual(len(attempts), 2)
        self.assertEqual(stream.size(), 0)

    def test_retry_callback_counts_events_when_retry_is_attempted(self):
        stream = EventStream()
        attempts = []
        retried_event_counts = []

        def handler(batch):
            attempts.append(batch)
            if len(attempts) == 1:
                raise RuntimeError("simulated analysis failure")

        processor = MicroBatchProcessor(
            event_stream=stream,
            batch_size=2,
            batch_handler=handler,
            on_retry=retried_event_counts.append,
        )

        stream.publish({"event_id": "event-1"})
        stream.publish({"event_id": "event-2"})

        with self.assertRaisesRegex(
            RuntimeError,
            "simulated analysis failure",
        ):
            processor.process_once()

        self.assertEqual(retried_event_counts, [])
        self.assertEqual(stream.size(), 2)

        result = processor.process_once()

        self.assertEqual(len(result), 2)
        self.assertEqual(retried_event_counts, [2])
        self.assertEqual(len(attempts), 2)
        self.assertEqual(stream.size(), 0)

    def test_id_handler_reuses_batch_id_when_batch_is_retried(self):
        stream = EventStream()
        attempts = []
        legacy_calls = []

        def handler_with_id(batch, batch_id):
            attempts.append(batch_id)
            if len(attempts) == 1:
                raise RuntimeError("simulated analysis failure")

        processor = MicroBatchProcessor(
            event_stream=stream,
            batch_size=1,
            batch_handler=legacy_calls.append,
            batch_handler_with_id=handler_with_id,
        )
        stream.publish({"event_id": "event-1"})

        with self.assertRaisesRegex(
            RuntimeError,
            "simulated analysis failure",
        ):
            processor.process_once()

        processor.process_once()

        self.assertEqual(len(attempts), 2)
        self.assertTrue(attempts[0])
        self.assertEqual(attempts[0], attempts[1])
        self.assertEqual(legacy_calls, [])

    def test_timeout_starts_when_event_arrives(self):
        clock = FakeClock()
        stream = EventStream(clock=clock)
        processed_batches = []

        processor = MicroBatchProcessor(
            event_stream=stream,
            batch_size=3,
            max_wait_seconds=10,
            batch_handler=processed_batches.append,
            clock=clock,
        )

        stream.publish({"event_id": "event-1"})

        clock.advance(9)
        result = processor.process_once()
        self.assertEqual(result, [])

        clock.advance(1)
        result = processor.process_once()

        self.assertEqual(result, [{"event_id": "event-1"}])
        self.assertEqual(len(processed_batches), 1)


if __name__ == "__main__":
    unittest.main()
