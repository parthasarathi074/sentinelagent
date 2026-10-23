"""Optional Kafka transport adapters; core SentinelAgent stays dependency-free.

Install with ``pip install 'sentinelagent[kafka]'``. Events are JSON-serialized,
keyed by edge_id for partition affinity, and include delivery callbacks. This
module does not silently replace the current in-process event stream: wire it in
through an explicit deployment adapter/configuration.
"""
from __future__ import annotations

import json
from typing import Any, Callable


class KafkaUnavailable(RuntimeError):
    pass


def _load_kafka():
    try:
        from confluent_kafka import Consumer, Producer  # type: ignore[import-not-found]
    except ImportError as exc:
        raise KafkaUnavailable("Kafka transport requires: pip install 'sentinelagent[kafka]'") from exc
    return Producer, Consumer


class KafkaEventPublisher:
    """Publish structured events to Kafka, partition-keyed by ``edge_id``."""

    def __init__(self, bootstrap_servers: str, topic: str = "sentinelagent-events", *, producer=None,
                 client_id: str = "sentinelagent") -> None:
        if not bootstrap_servers.strip() or not topic.strip():
            raise ValueError("bootstrap_servers and topic are required")
        if producer is None:
            Producer, _ = _load_kafka()
            producer = Producer({"bootstrap.servers": bootstrap_servers, "client.id": client_id,
                                 "enable.idempotence": True, "acks": "all"})
        self.producer, self.topic = producer, topic

    def publish(self, event: dict[str, Any], *, edge_id: str, callback: Callable[[Exception | None], None] | None = None) -> None:
        if not edge_id:
            raise ValueError("edge_id is required to preserve partition affinity")
        payload = json.dumps(event, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        def delivered(error, _message):
            if callback:
                callback(error)
        self.producer.produce(self.topic, key=edge_id.encode("utf-8"), value=payload, callback=delivered)
        self.producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        remaining = self.producer.flush(timeout)
        if remaining:
            raise TimeoutError(f"{remaining} Kafka message(s) were not delivered before timeout")
        return remaining


class KafkaEventConsumer:
    """Small poll-based consumer returning decoded event dictionaries."""

    def __init__(self, bootstrap_servers: str, group_id: str, topic: str, *, consumer=None,
                 auto_offset_reset: str = "earliest") -> None:
        if not group_id.strip() or not topic.strip() or not bootstrap_servers.strip():
            raise ValueError("bootstrap_servers, group_id, and topic are required")
        if consumer is None:
            _, Consumer = _load_kafka()
            consumer = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": group_id,
                                 "auto.offset.reset": auto_offset_reset, "enable.auto.commit": False})
        self.consumer, self.topic = consumer, topic
        self.consumer.subscribe([topic])

    def poll(self, timeout: float = 1.0) -> dict[str, Any] | None:
        message = self.consumer.poll(timeout)
        if message is None:
            return None
        if message.error():
            raise RuntimeError(f"Kafka consumer error: {message.error()}")
        value = json.loads(message.value().decode("utf-8"))
        if not isinstance(value, dict):
            raise ValueError("Kafka event payload must decode to a JSON object")
        return value

    def commit(self) -> None:
        self.consumer.commit(asynchronous=False)

    def close(self) -> None:
        self.consumer.close()
