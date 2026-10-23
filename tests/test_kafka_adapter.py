import json

import pytest

from sentinelagent.kafka_adapter import KafkaEventPublisher, KafkaEventConsumer


class FakeProducer:
    def __init__(self): self.messages = []; self.flush_called = False
    def produce(self, topic, key, value, callback): self.messages.append((topic, key, value, callback))
    def poll(self, timeout): pass
    def flush(self, timeout): self.flush_called = True; return 0


class FakeMessage:
    def error(self): return None
    def value(self): return b'{"event_id":"e-1"}'


class FakeConsumer:
    def subscribe(self, topics): self.topics = topics
    def poll(self, timeout): return FakeMessage()
    def commit(self, asynchronous=False): self.committed = not asynchronous
    def close(self): self.closed = True


def test_publisher_keys_by_edge_and_serializes_json():
    producer = FakeProducer()
    publisher = KafkaEventPublisher("localhost:9092", producer=producer)
    publisher.publish({"event_id": "e-1"}, edge_id="edge-A")
    topic, key, value, _ = producer.messages[0]
    assert topic == "sentinelagent-events"
    assert key == b"edge-A"
    assert json.loads(value) == {"event_id": "e-1"}
    assert publisher.flush() == 0


def test_publisher_requires_edge_id():
    publisher = KafkaEventPublisher("localhost:9092", producer=FakeProducer())
    with pytest.raises(ValueError): publisher.publish({}, edge_id="")


def test_consumer_decodes_and_commits_explicitly():
    consumer = FakeConsumer()
    adapter = KafkaEventConsumer("localhost:9092", "test-group", "events", consumer=consumer)
    assert adapter.poll() == {"event_id": "e-1"}
    adapter.commit()
    adapter.close()
    assert consumer.committed and consumer.closed
