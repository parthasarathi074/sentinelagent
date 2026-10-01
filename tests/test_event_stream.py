import unittest

from sentinelagent.event_stream import EventStream


class TestEventStream(unittest.TestCase):

    def test_publish_and_size(self):
        stream = EventStream()

        stream.publish("event-1")
        stream.publish("event-2")

        self.assertEqual(stream.size(), 2)

    def test_consume_batch(self):
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

        batch = stream.consume_batch(5)

        self.assertEqual(
            batch,
            [
                "event-0",
                "event-1",
                "event-2",
                "event-3",
                "event-4",
            ],
        )

    def test_consume_more_than_available(self):
        stream = EventStream()

        stream.publish("event-1")
        stream.publish("event-2")

        batch = stream.consume_batch(10)

        self.assertEqual(batch, ["event-1", "event-2"])
        self.assertEqual(stream.size(), 0)

    def test_invalid_batch_size(self):
        stream = EventStream()

        with self.assertRaises(ValueError):
            stream.consume_batch(0)

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


if __name__ == "__main__":
    unittest.main()