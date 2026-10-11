import unittest

from sentinelagent.event_stream import EventStream


class TestEventStream(unittest.TestCase):

    def test_publish_and_size(self):
        stream = EventStream()

        stream.publish("event-1")
        stream.publish("event-2")

        self.assertEqual(stream.size(), 2)

    def test_empty_stream_has_no_events_or_oldest_time(self):
        stream = EventStream()

        self.assertEqual(stream.size(), 0)
        self.assertIsNone(stream.oldest_event_time())
        self.assertEqual(stream.consume_batch(1), [])

    def test_publish_and_consume_batch(self):
        stream = EventStream()

        stream.publish("event-1")
        stream.publish("event-2")
        stream.publish("event-3")

        batch = stream.consume_batch(2)

        self.assertEqual(batch, ["event-1", "event-2"])
        self.assertEqual(stream.size(), 1)

    def test_consume_preserves_order(self):
        stream = EventStream()

        for number in range(5):
            stream.publish(f"event-{number}")

        self.assertEqual(
            stream.consume_batch(5),
            [
                "event-0",
                "event-1",
                "event-2",
                "event-3",
                "event-4",
            ],
        )

    def test_consume_exact_batch_boundary(self):
        stream = EventStream()

        for number in range(6):
            stream.publish(f"event-{number}")

        first_batch = stream.consume_batch(3)
        second_batch = stream.consume_batch(3)

        self.assertEqual(first_batch, ["event-0", "event-1", "event-2"])
        self.assertEqual(second_batch, ["event-3", "event-4", "event-5"])
        self.assertEqual(stream.size(), 0)

    def test_consume_more_than_available(self):
        stream = EventStream()

        stream.publish("event-1")
        stream.publish("event-2")

        self.assertEqual(
            stream.consume_batch(10),
            ["event-1", "event-2"],
        )
        self.assertEqual(stream.size(), 0)

    def test_consume_large_volume_preserves_every_event_in_order(self):
        stream = EventStream()
        expected = [f"event-{number}" for number in range(1000)]

        for event in expected:
            stream.publish(event)

        consumed = []
        while stream.size() > 0:
            consumed.extend(stream.consume_batch(37))

        self.assertEqual(consumed, expected)
        self.assertEqual(len(consumed), 1000)
        self.assertEqual(stream.size(), 0)

    def test_invalid_batch_size(self):
        stream = EventStream()

        with self.assertRaises(ValueError):
            stream.consume_batch(0)

    def test_negative_batch_size_is_rejected(self):
        stream = EventStream()

        with self.assertRaises(ValueError):
            stream.consume_batch(-1)

    def test_non_integer_batch_sizes_are_rejected(self):
        stream = EventStream()

        for invalid_size in (1.5, "2", None, True, False):
            with self.subTest(invalid_size=invalid_size):
                with self.assertRaises(ValueError):
                    stream.consume_batch(invalid_size)

    def test_restore_empty_batch_does_nothing(self):
        stream = EventStream()
        stream.publish("event-1")

        stream.restore_batch([])

        self.assertEqual(stream.size(), 1)
        self.assertEqual(stream.consume_batch(1), ["event-1"])

    def test_restore_batch_preserves_order_at_front(self):
        stream = EventStream()
        stream.publish({"event_id": "event-3"})

        stream.restore_batch([
            {"event_id": "event-1"},
            {"event_id": "event-2"},
        ])

        self.assertEqual(
            stream.consume_batch(3),
            [
                {"event_id": "event-1"},
                {"event_id": "event-2"},
                {"event_id": "event-3"},
            ],
        )

    def test_restore_preserves_original_arrival_time(self):
        current_time = [10.0]
        stream = EventStream(clock=lambda: current_time[0])

        stream.publish("event-1")
        original_arrival_time = stream.oldest_event_time()

        batch = stream.consume_batch(1)
        current_time[0] = 50.0
        stream.restore_batch(batch)

        self.assertEqual(stream.oldest_event_time(), original_arrival_time)
        self.assertEqual(stream.consume_batch(1), ["event-1"])

    def test_restore_preserves_order_and_timestamps(self):
        current_time = [10.0]
        stream = EventStream(clock=lambda: current_time[0])

        stream.publish("event-1")
        current_time[0] = 20.0
        stream.publish("event-2")
        current_time[0] = 30.0
        stream.publish("event-3")

        batch = stream.consume_batch(2)
        current_time[0] = 100.0
        stream.restore_batch(batch)

        self.assertEqual(stream.oldest_event_time(), 10.0)
        self.assertEqual(
            stream.consume_batch(3),
            ["event-1", "event-2", "event-3"],
        )
        self.assertIsNone(stream.oldest_event_time())

    def test_restore_multiple_batches_preserves_each_timestamp(self):
        current_time = [10.0]
        stream = EventStream(clock=lambda: current_time[0])

        stream.publish("event-1")
        first_batch = stream.consume_batch(1)

        current_time[0] = 20.0
        stream.publish("event-2")
        second_batch = stream.consume_batch(1)

        current_time[0] = 100.0
        stream.restore_batch(first_batch)
        stream.restore_batch(second_batch)

        self.assertEqual(stream.size(), 2)
        self.assertEqual(stream.oldest_event_time(), 10.0)
        self.assertEqual(
            stream.consume_batch(2),
            ["event-1", "event-2"],
        )

    def test_restore_multiple_batches_with_equal_timestamps_preserves_order(self):
        current_time = [10.0]
        stream = EventStream(clock=lambda: current_time[0])

        stream.publish("event-1")
        first_batch = stream.consume_batch(1)

        stream.publish("event-2")
        second_batch = stream.consume_batch(1)

        stream.restore_batch(first_batch)
        stream.restore_batch(second_batch)

        self.assertEqual(
            stream.consume_batch(2),
            ["event-1", "event-2"],
        )

    def test_restore_batches_in_reverse_order_with_equal_timestamps(self):
        current_time = [10.0]
        stream = EventStream(clock=lambda: current_time[0])

        stream.publish("event-1")
        first_batch = stream.consume_batch(1)

        stream.publish("event-2")
        second_batch = stream.consume_batch(1)

        stream.restore_batch(second_batch)
        stream.restore_batch(first_batch)

        self.assertEqual(
            stream.consume_batch(2),
            ["event-1", "event-2"],
        )


if __name__ == "__main__":
    unittest.main()
