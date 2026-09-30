import unittest

from sentinelagent.analysis import AnalysisEngine
from sentinelagent.batch_worker import BatchWorker


class TestBatchWorker(unittest.TestCase):

    def test_processes_batch_with_analysis_handler(self):
        processed_batches = []

        def analysis_handler(batch):
            processed_batches.append(batch)
            return "analysis-complete"

        worker = BatchWorker(
            batch_handler=analysis_handler,
        )

        batch = [
            {"event_id": "event-1"},
            {"event_id": "event-2"},
        ]

        result = worker.process(batch)

        self.assertEqual(result, "analysis-complete")
        self.assertEqual(processed_batches, [batch])

    def test_batch_worker_can_use_analysis_engine(self):
        engine = AnalysisEngine()

        worker = BatchWorker(
            batch_handler=engine.analyze,
        )

        batch = [
            {"event_id": "event-101"},
            {"event_id": "event-102"},
            {"event_id": "event-103"},
        ]

        result = worker.process(batch)

        self.assertEqual(result["event_count"], 3)
        self.assertEqual(
            result["event_ids"],
            ["event-101", "event-102", "event-103"],
        )

    def test_empty_batch_is_rejected(self):
        worker = BatchWorker(
            batch_handler=lambda batch: None,
        )

        with self.assertRaises(ValueError):
            worker.process([])

    def test_handler_receives_original_batch(self):
        received_batch = []

        def analysis_handler(batch):
            received_batch.extend(batch)

        worker = BatchWorker(
            batch_handler=analysis_handler,
        )

        batch = [
            {"event_id": "event-1"},
            {"event_id": "event-2"},
        ]

        worker.process(batch)

        self.assertEqual(received_batch, batch)

    def test_micro_batch_reaches_batch_worker_and_analysis_handler(self):
        from sentinelagent.event_stream import EventStream
        from sentinelagent.micro_batch import MicroBatchProcessor

        stream = EventStream()
        processed_batches = []

        def analysis_handler(batch):
            processed_batches.append(batch)

        worker = BatchWorker(
            batch_handler=analysis_handler,
        )

        processor = MicroBatchProcessor(
            event_stream=stream,
            batch_size=2,
            batch_handler=worker.process,
        )

        stream.publish({"event_id": "event-1"})
        stream.publish({"event_id": "event-2"})

        result = processor.process_once()

        expected_batch = [
            {"event_id": "event-1"},
            {"event_id": "event-2"},
        ]

        self.assertEqual(result, expected_batch)
        self.assertEqual(processed_batches, [expected_batch])
        self.assertEqual(stream.size(), 0)


if __name__ == "__main__":
    unittest.main()