from __future__ import annotations

import signal
import time
from uuid import uuid4

from alerts.severity import classify_severity, should_open_incident
from alerts.sink import get_sink
from common.config import settings
from common.db import Json, close_pool, get_conn
from common.logging import get_logger
from common.migrate import apply_migrations
from explain import explain_anomaly

log = get_logger("alerts.engine")
_running = True

CANDIDATES_SQL = """
SELECT
    a.cell_id,
    a.event_ts,
    a.anomaly_score,
    a.model_version,
    m.site_id,
    m.region,
    m.technology,
    m.active_users,
    m.throughput_mbps,
    m.latency_ms,
    m.packet_loss_pct,
    m.signal_strength_dbm,
    m.cpu_usage_pct,
    m.memory_usage_pct,
    m.network_status,
    c.max_capacity_users
FROM ml.anomalies a
JOIN clean.network_metrics m
    ON m.cell_id = a.cell_id AND m.event_ts = a.event_ts
JOIN ops.cells c ON c.cell_id = a.cell_id
WHERE a.is_anomaly
  AND a.event_ts > NOW() - (%s * INTERVAL '1 minute')
  AND NOT EXISTS (
        SELECT 1 FROM ops.alerts al
        WHERE al.cell_id = a.cell_id AND al.event_ts = a.event_ts
  )
  AND NOT EXISTS (
        SELECT 1 FROM ops.alerts al
        WHERE al.cell_id = a.cell_id
          AND al.created_ts > NOW() - (%s * INTERVAL '1 minute')
  )
ORDER BY a.event_ts DESC
LIMIT 40
"""


def _handle_stop(signum, _frame) -> None:
    global _running
    log.info("shutdown_signal", extra={"event": "shutdown", "count": signum})
    _running = False


def _row_metrics(row: dict) -> dict:
    return {
        "cell_id": row["cell_id"],
        "site_id": row["site_id"],
        "region": row["region"],
        "technology": row["technology"],
        "active_users": row["active_users"],
        "throughput_mbps": row["throughput_mbps"],
        "latency_ms": row["latency_ms"],
        "packet_loss_pct": row["packet_loss_pct"],
        "signal_strength_dbm": row["signal_strength_dbm"],
        "cpu_usage_pct": row["cpu_usage_pct"],
        "memory_usage_pct": row["memory_usage_pct"],
        "network_status": row["network_status"],
        "max_capacity_users": row["max_capacity_users"],
    }


def process_once() -> int:
    lookback = int(settings.alert_lookback_minutes)
    cooldown = int(settings.alert_cooldown_minutes)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CANDIDATES_SQL, (lookback, cooldown))
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    if not rows:
        return 0

    sink = get_sink()
    created = 0
    for row in rows:
        metrics = _row_metrics(row)
        severity = classify_severity(metrics, is_anomaly=True)
        if severity is None:
            continue
        explanation = explain_anomaly(metrics)
        alert_id = f"ALT-{uuid4().hex[:10].upper()}"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ops.alerts (
                        alert_id, event_ts, site_id, cell_id, severity, alert_type,
                        metrics, anomaly_score, possible_cause, evidence,
                        recommended_action
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (cell_id, event_ts) DO NOTHING
                    """,
                    (
                        alert_id,
                        row["event_ts"],
                        row["site_id"],
                        row["cell_id"],
                        severity,
                        explanation["possible_cause"],
                        Json(metrics),
                        row["anomaly_score"],
                        explanation["possible_cause"],
                        Json(explanation),
                        explanation["recommended_action"],
                    ),
                )
                if cur.rowcount == 0:
                    continue
        if should_open_incident(severity):
            ticket = sink.create_incident(
                {
                    "alert_id": alert_id,
                    "cell_id": row["cell_id"],
                    "site_id": row["site_id"],
                    "severity": severity,
                    "anomaly_score": row["anomaly_score"],
                    **explanation,
                }
            )
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE ops.alerts
                        SET servicenow_number = %s, servicenow_sys_id = %s, sink = %s
                        WHERE alert_id = %s
                        """,
                        (
                            ticket.get("number"),
                            ticket.get("sys_id"),
                            ticket.get("sink"),
                            alert_id,
                        ),
                    )
        created += 1
    if created:
        log.info("alerts_created", extra={"event": "alert", "count": created})
    return created


def run() -> None:
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)
    apply_migrations()
    log.info("alert_engine_started", extra={"event": "alert"})
    while _running:
        try:
            process_once()
        except Exception as exc:  # noqa: BLE001
            log.warning("alert_loop_error", extra={"event": "alert", "error": str(exc)})
        for _ in range(int(settings.alert_interval_seconds)):
            if not _running:
                break
            time.sleep(1)
    close_pool()


if __name__ == "__main__":
    run()
