from __future__ import annotations

import json
import time

from confluent_kafka import KafkaException, Producer

from common.config import settings
from common.logging import get_logger

log = get_logger("simulator.producer")


class MetricsProducer:
    def __init__(self) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 20,
                "batch.size": 32768,
                "retries": 8,
            }
        )

    def wait_for_kafka(self, timeout_s: int = 90) -> None:
        deadline = time.time() + timeout_s
        last: Exception | None = None
        while time.time() < deadline:
            try:
                self._producer.list_topics(timeout=5)
                log.info("kafka_ready", extra={"event": "kafka_ready"})
                return
            except KafkaException as exc:
                last = exc
                time.sleep(2)
        raise RuntimeError(f"kafka not reachable: {last}")

    def send(self, event: dict, key: str | None) -> None:
        payload = json.dumps(event, default=str).encode("utf-8")
        k = (key or event.get("cell_id") or "unknown").encode("utf-8")
        self._producer.produce(
            settings.metrics_topic,
            value=payload,
            key=k,
        )

    def poll(self) -> None:
        self._producer.poll(0)

    def flush(self, timeout: float = 10) -> None:
        self._producer.flush(timeout)
