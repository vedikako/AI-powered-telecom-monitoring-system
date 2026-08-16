from __future__ import annotations

import json
import signal
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

from common.config import settings
from common.db import close_pool, get_conn
from common.logging import get_logger
from common.schemas import ValidationFailure
from pipeline.quality import QualityTracker
from pipeline.validator import validate_payload
from pipeline.writer import insert_batch

log = get_logger("pipeline")
_running = True
HEARTBEAT = Path("/tmp/processor_healthy")


def _handle_stop(signum, _frame) -> None:
    global _running
    log.info("shutdown_signal", extra={"event": "shutdown", "count": signum})
    _running = False


def _heartbeat() -> None:
    try:
        HEARTBEAT.write_text(str(time.time()), encoding="utf-8")
    except OSError:
        pass


def wait_for_kafka(consumer: Consumer, timeout_s: int = 90) -> None:
    deadline = time.time() + timeout_s
    last: Exception | None = None
    while time.time() < deadline:
        try:
            consumer.list_topics(timeout=5)
            return
        except KafkaException as exc:
            last = exc
            time.sleep(2)
    raise RuntimeError(f"kafka not reachable: {last}")


def consumer_lag(consumer: Consumer) -> int | None:
    assignment = consumer.assignment()
    if not assignment:
        return None
    total = 0
    try:
        for tp in assignment:
            low, high = consumer.get_watermark_offsets(tp, timeout=2)
            committed = consumer.committed([TopicPartition(tp.topic, tp.partition)], timeout=2)
            offset = committed[0].offset if committed and committed[0].offset is not None else -1
            if offset < 0:
                total += max(high - low, 0)
            else:
                total += max(high - offset, 0)
        return total
    except KafkaException:
        return None


def _send_dlq(producer: Producer, failure, partition: int | None, offset: int | None, raw: bytes | None) -> None:
    body = {
        "error_class": failure.error_class,
        "error_reason": failure.error_reason,
        "payload": failure.payload,
        "source_partition": partition,
        "source_offset": offset,
    }
    producer.produce(
        settings.dlq_topic,
        value=json.dumps(body, default=str).encode("utf-8"),
        key=str((failure.payload or {}).get("cell_id") or "unknown").encode("utf-8"),
    )


def run() -> None:
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": settings.consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300000,
        }
    )
    dlq = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "acks": "all",
            "enable.idempotence": True,
        }
    )
    wait_for_kafka(consumer)
    known_cells = _load_cell_ids()
    consumer.subscribe([settings.metrics_topic])
    log.info(
        "consumer_started",
        extra={"event": "consume", "topic": settings.metrics_topic, "count": len(known_cells)},
    )

    quality = QualityTracker()
    pending_valid: list = []
    pending_invalid: list = []
    last_batch = time.time()

    try:
        while _running:
            msg = consumer.poll(0.5)
            _heartbeat()
            now = time.time()

            if msg is None:
                if pending_valid or pending_invalid:
                    if now - last_batch >= settings.batch_wait_seconds:
                        _flush(consumer, dlq, quality, pending_valid, pending_invalid)
                        pending_valid, pending_invalid = [], []
                        last_batch = now
                quality.maybe_flush(settings.quality_interval_seconds, consumer_lag(consumer))
                continue

            if msg.error():
                log.warning("kafka_error", extra={"event": "kafka_error", "error": str(msg.error())})
                continue

            quality.mark_received()
            start = time.perf_counter()
            event, failure = validate_payload(msg.value())
            latency_ms = (time.perf_counter() - start) * 1000
            quality.set_latency(latency_ms)

            if event is not None and event.cell_id not in known_cells:
                failure = ValidationFailure(
                    error_class="schema_fail",
                    error_reason=f"unknown cell_id: {event.cell_id}",
                    payload=event.model_dump(mode="json"),
                )
                event = None
            if failure is not None:
                quality.mark_invalid(failure.error_class)
                pending_invalid.append((failure, msg.partition(), msg.offset()))
                _send_dlq(dlq, failure, msg.partition(), msg.offset(), msg.value())
            else:
                quality.mark_valid()
                payload = event.model_dump(mode="json")
                pending_valid.append((event, msg.partition(), msg.offset(), payload))

            full = len(pending_valid) + len(pending_invalid) >= settings.batch_size
            stale = now - last_batch >= settings.batch_wait_seconds
            if full or stale:
                _flush(consumer, dlq, quality, pending_valid, pending_invalid)
                pending_valid, pending_invalid = [], []
                last_batch = time.time()
            quality.maybe_flush(settings.quality_interval_seconds, consumer_lag(consumer))
    finally:
        if pending_valid or pending_invalid:
            _flush(consumer, dlq, quality, pending_valid, pending_invalid)
        consumer.close()
        dlq.flush(5)
        close_pool()
        log.info("processor_stopped", extra={"event": "shutdown"})


def _load_cell_ids() -> set[str]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT cell_id FROM ops.cells")
            return {row[0] for row in cur.fetchall()}


def _flush(consumer, dlq, quality: QualityTracker, valid, invalid) -> None:
    if not valid and not invalid:
        return
    for attempt in range(1, 6):
        try:
            _raw, _clean, dups = insert_batch(valid, invalid)
            quality.mark_duplicate(dups)
            dlq.flush(5)
            consumer.commit(asynchronous=False)
            return
        except Exception as exc:  # noqa: BLE001
            quality.mark_db_error()
            log.warning(
                "batch_retry",
                extra={"event": "db_retry", "error": str(exc), "count": attempt},
            )
            time.sleep(min(2**attempt, 10))
    log.error("batch_failed", extra={"event": "db_error", "error": "exhausted retries"})


if __name__ == "__main__":
    run()
