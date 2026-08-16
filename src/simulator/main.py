from __future__ import annotations

import signal
import time
from datetime import datetime, timedelta, timezone

import numpy as np

from common.config import settings
from common.db import close_pool, get_conn
from common.logging import get_logger
from simulator.incidents import IncidentManager
from simulator.physics import corrupt_event, generate_event
from simulator.producer import MetricsProducer
from simulator.topology import load_cells_from_db

log = get_logger("simulator")
_running = True


def _handle_stop(signum, _frame) -> None:
    global _running
    log.info("shutdown_signal", extra={"event": "shutdown", "count": signum})
    _running = False


def _persist_started(started) -> None:
    if not started:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            for inc in started:
                cur.execute(
                    """
                    INSERT INTO ops.injected_incidents
                        (incident_id, cell_id, incident_type, start_ts, end_ts, severity)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (incident_id) DO NOTHING
                    """,
                    (
                        inc.incident_id,
                        inc.cell_id,
                        inc.incident_type,
                        inc.start_ts,
                        inc.end_ts,
                        inc.severity,
                    ),
                )


def _persist_ended(ended) -> None:
    if not ended:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            for inc in ended:
                cur.execute(
                    """
                    UPDATE ops.injected_incidents
                    SET end_ts = %s
                    WHERE incident_id = %s
                    """,
                    (inc.end_ts, inc.incident_id),
                )


def _emit_tick(cells, ts, rng, manager, producer, interval: float) -> int:
    effects = manager.tick(ts)
    _persist_started(manager.started)
    _persist_ended(manager.ended)
    sent = 0
    for cell in cells:
        event = generate_event(cell, ts, rng, effects.get(cell.cell_id))
        if rng.random() < settings.invalid_rate:
            event = corrupt_event(event, rng)
            key = event.get("cell_id") or cell.cell_id
        else:
            key = cell.cell_id
        producer.send(event, key=key)
        sent += 1
    producer.poll()
    return sent


def run() -> None:
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    rng = np.random.default_rng(7)
    cells = load_cells_from_db()
    if not cells:
        raise RuntimeError("no cells in ops.cells; did seed run?")
    if len(cells) != 100:
        log.warning("unexpected_cell_count", extra={"event": "topology", "count": len(cells)})
    manager = IncidentManager(cells, rng, settings.interval_seconds)
    producer = MetricsProducer()
    producer.wait_for_kafka()

    now = datetime.now(timezone.utc).replace(microsecond=0)
    backfill_start = now - timedelta(hours=settings.backfill_hours)
    ts = backfill_start
    emitted = 0

    log.info(
        "backfill_start",
        extra={"event": "backfill_start", "count": int(settings.backfill_hours)},
    )
    while _running and ts < now:
        emitted += _emit_tick(cells, ts, rng, manager, producer, settings.interval_seconds)
        ts += timedelta(seconds=settings.interval_seconds)
        if emitted % 5000 == 0:
            producer.flush()
            log.info("backfill_progress", extra={"event": "backfill", "count": emitted})

    producer.flush()
    log.info("backfill_done", extra={"event": "backfill_done", "count": emitted})

    while _running:
        tick_start = time.time()
        live_ts = datetime.now(timezone.utc).replace(microsecond=0)
        emitted += _emit_tick(cells, live_ts, rng, manager, producer, settings.interval_seconds)
        producer.flush(1)
        elapsed = time.time() - tick_start
        sleep_for = max(0.0, settings.interval_seconds - elapsed)
        time.sleep(sleep_for)

    producer.flush()
    close_pool()
    log.info("simulator_stopped", extra={"event": "shutdown", "count": emitted})


if __name__ == "__main__":
    run()
