from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from confluent_kafka import Consumer, Producer, KafkaException

from common.config import settings
from common.logging import get_logger

log = get_logger("replay_dlq")


def main() -> None:
    """Replay DLQ messages onto the metrics topic for investigation/retry."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": "dlq-replay",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap, "acks": "all"})
    consumer.subscribe([settings.dlq_topic])
    replayed = 0
    idle = 0
    try:
        while idle < 10:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1
                continue
            if msg.error():
                raise KafkaException(msg.error())
            idle = 0
            body = json.loads(msg.value().decode("utf-8"))
            original = body.get("payload")
            if not original:
                continue
            key = str(original.get("cell_id") or "unknown").encode("utf-8")
            producer.produce(settings.metrics_topic, value=json.dumps(original).encode("utf-8"), key=key)
            replayed += 1
        producer.flush()
        consumer.commit(asynchronous=False)
    finally:
        consumer.close()
    log.info("dlq_replayed", extra={"event": "replay", "count": replayed})
    print(f"replayed {replayed} records")


if __name__ == "__main__":
    main()
