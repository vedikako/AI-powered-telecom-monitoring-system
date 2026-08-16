from __future__ import annotations

import signal
import time

from common.config import settings
from common.db import close_pool, get_conn
from common.logging import get_logger

log = get_logger("batch.etl")
_running = True

HOURLY_SQL = """
WITH hourly AS (
    SELECT
        m.cell_id,
        date_trunc('hour', m.event_ts) AS hour_ts,
        MIN(m.site_id) AS site_id,
        MIN(m.region) AS region,
        MIN(m.technology) AS technology,
        AVG(m.latency_ms) AS avg_latency_ms,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY m.latency_ms) AS p95_latency_ms,
        AVG(m.throughput_mbps) AS avg_throughput_mbps,
        AVG(m.packet_loss_pct) AS avg_packet_loss_pct,
        MAX(m.active_users) AS max_active_users,
        AVG(m.cpu_usage_pct) AS avg_cpu_usage_pct,
        COUNT(*)::int AS sample_count
    FROM clean.network_metrics m
    GROUP BY m.cell_id, date_trunc('hour', m.event_ts)
),
anom AS (
    SELECT
        cell_id,
        date_trunc('hour', event_ts) AS hour_ts,
        COUNT(*)::int AS anomaly_count
    FROM ml.anomalies
    WHERE is_anomaly
    GROUP BY cell_id, date_trunc('hour', event_ts)
)
INSERT INTO analytics.hourly_cell_metrics (
    cell_id, hour_ts, site_id, region, technology,
    avg_latency_ms, p95_latency_ms, avg_throughput_mbps, avg_packet_loss_pct,
    max_active_users, avg_cpu_usage_pct, sample_count, anomaly_count
)
SELECT
    h.cell_id, h.hour_ts, h.site_id, h.region, h.technology,
    h.avg_latency_ms, h.p95_latency_ms, h.avg_throughput_mbps, h.avg_packet_loss_pct,
    h.max_active_users, h.avg_cpu_usage_pct, h.sample_count,
    COALESCE(a.anomaly_count, 0)
FROM hourly h
LEFT JOIN anom a ON a.cell_id = h.cell_id AND a.hour_ts = h.hour_ts
ON CONFLICT (cell_id, hour_ts) DO UPDATE SET
    site_id = EXCLUDED.site_id,
    region = EXCLUDED.region,
    technology = EXCLUDED.technology,
    avg_latency_ms = EXCLUDED.avg_latency_ms,
    p95_latency_ms = EXCLUDED.p95_latency_ms,
    avg_throughput_mbps = EXCLUDED.avg_throughput_mbps,
    avg_packet_loss_pct = EXCLUDED.avg_packet_loss_pct,
    max_active_users = EXCLUDED.max_active_users,
    avg_cpu_usage_pct = EXCLUDED.avg_cpu_usage_pct,
    sample_count = EXCLUDED.sample_count,
    anomaly_count = EXCLUDED.anomaly_count
"""

DAILY_SQL = """
WITH daily AS (
    SELECT
        m.site_id,
        (m.event_ts AT TIME ZONE 'UTC')::date AS day_ts,
        MIN(m.region) AS region,
        AVG(m.latency_ms) AS avg_latency_ms,
        AVG(m.throughput_mbps) AS avg_throughput_mbps,
        AVG(m.packet_loss_pct) AS avg_packet_loss_pct,
        MAX(m.active_users) AS max_active_users,
        SUM(CASE WHEN m.network_status = 'DOWN' THEN 10.0 / 60.0 ELSE 0 END) AS outage_minutes
    FROM clean.network_metrics m
    GROUP BY m.site_id, (m.event_ts AT TIME ZONE 'UTC')::date
),
anom AS (
    SELECT
        c.site_id,
        (a.event_ts AT TIME ZONE 'UTC')::date AS day_ts,
        COUNT(*)::int AS anomaly_count
    FROM ml.anomalies a
    JOIN ops.cells c ON c.cell_id = a.cell_id
    WHERE a.is_anomaly
    GROUP BY c.site_id, (a.event_ts AT TIME ZONE 'UTC')::date
)
INSERT INTO analytics.daily_site_metrics (
    site_id, day_ts, region, avg_latency_ms, avg_throughput_mbps,
    avg_packet_loss_pct, max_active_users, outage_minutes, anomaly_count
)
SELECT
    d.site_id, d.day_ts, d.region, d.avg_latency_ms, d.avg_throughput_mbps,
    d.avg_packet_loss_pct, d.max_active_users, d.outage_minutes,
    COALESCE(a.anomaly_count, 0)
FROM daily d
LEFT JOIN anom a ON a.site_id = d.site_id AND a.day_ts = d.day_ts
ON CONFLICT (site_id, day_ts) DO UPDATE SET
    region = EXCLUDED.region,
    avg_latency_ms = EXCLUDED.avg_latency_ms,
    avg_throughput_mbps = EXCLUDED.avg_throughput_mbps,
    avg_packet_loss_pct = EXCLUDED.avg_packet_loss_pct,
    max_active_users = EXCLUDED.max_active_users,
    outage_minutes = EXCLUDED.outage_minutes,
    anomaly_count = EXCLUDED.anomaly_count
"""


def _handle_stop(signum, _frame) -> None:
    global _running
    log.info("shutdown_signal", extra={"event": "shutdown", "count": signum})
    _running = False


def run_once() -> tuple[int, int]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(HOURLY_SQL)
            hourly = cur.rowcount
            cur.execute(DAILY_SQL)
            daily = cur.rowcount
    log.info("etl_complete", extra={"event": "etl", "count": hourly})
    return hourly, daily


def run() -> None:
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)
    while _running:
        try:
            run_once()
        except Exception as exc:  # noqa: BLE001
            log.warning("etl_failed", extra={"event": "etl_error", "error": str(exc)})
        for _ in range(int(settings.etl_interval_seconds)):
            if not _running:
                break
            time.sleep(1)
    close_pool()


if __name__ == "__main__":
    run()
