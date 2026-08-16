from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException

from common.db import get_conn
from common.migrate import apply_migrations
from explain import explain_anomaly

app = FastAPI(
    title="Telecom Network Analytics API",
    version="0.2.0",
    description="Read APIs for cell analysis, alerts, and pipeline health. Optional; Grafana still reads Postgres directly.",
)


@app.on_event("startup")
def _startup() -> None:
    apply_migrations()


def _query(sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description is None:
                return []
            cols = [d[0] for d in cur.description]
            return [_jsonable(dict(zip(cols, row))) for row in cur.fetchall()]


def _one(sql: str, params: tuple = ()) -> dict[str, Any] | None:
    rows = _query(sql, params)
    return rows[0] if rows else None


def _jsonable(obj: dict[str, Any]) -> dict[str, Any]:
    out = {}
    for k, v in obj.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


@app.get("/health")
def health() -> dict[str, str]:
    _one("SELECT 1 AS ok")
    return {"status": "ok"}


@app.get("/network-health")
def network_health() -> dict[str, Any]:
    row = _one(
        """
        SELECT
            COUNT(DISTINCT cell_id) AS active_cells,
            ROUND((100.0 * SUM(CASE WHEN network_status = 'UP' THEN 1 ELSE 0 END)
                   / NULLIF(COUNT(*), 0))::numeric, 1) AS health_pct,
            ROUND(AVG(latency_ms)::numeric, 1) AS avg_latency_ms,
            ROUND(AVG(throughput_mbps)::numeric, 1) AS avg_throughput_mbps,
            ROUND(AVG(packet_loss_pct)::numeric, 2) AS avg_packet_loss_pct
        FROM clean.network_metrics
        WHERE event_ts > NOW() - INTERVAL '15 minutes'
        """
    ) or {}
    open_alerts = _one(
        """
        SELECT COUNT(*) AS active_alerts
        FROM ops.alerts
        WHERE created_ts > NOW() - INTERVAL '15 minutes'
        """
    ) or {}
    return {**row, **open_alerts}


@app.get("/cells")
def list_cells() -> list[dict[str, Any]]:
    return _query(
        """
        SELECT c.cell_id, c.site_id, s.region, c.technology, c.band, c.max_capacity_users
        FROM ops.cells c
        JOIN ops.sites s ON s.site_id = c.site_id
        ORDER BY c.cell_id
        """
    )


@app.get("/sites")
def list_sites() -> list[dict[str, Any]]:
    return _query("SELECT site_id, region, site_type, latitude, longitude FROM ops.sites ORDER BY site_id")


@app.get("/cells/{cell_id}/analysis")
def cell_analysis(cell_id: str) -> dict[str, Any]:
    current = _one(
        """
        SELECT m.*, c.max_capacity_users
        FROM clean.network_metrics m
        JOIN ops.cells c ON c.cell_id = m.cell_id
        WHERE m.cell_id = %s
        ORDER BY m.event_ts DESC
        LIMIT 1
        """,
        (cell_id,),
    )
    if current is None:
        raise HTTPException(status_code=404, detail=f"no metrics for {cell_id}")
    history = _query(
        """
        SELECT event_ts, active_users, throughput_mbps, latency_ms, packet_loss_pct,
               signal_strength_dbm, cpu_usage_pct, memory_usage_pct, network_status
        FROM clean.network_metrics
        WHERE cell_id = %s
        ORDER BY event_ts DESC
        LIMIT 30
        """,
        (cell_id,),
    )
    anomaly = _one(
        """
        SELECT event_ts, anomaly_score, is_anomaly, model_version
        FROM ml.anomalies
        WHERE cell_id = %s
        ORDER BY event_ts DESC
        LIMIT 1
        """,
        (cell_id,),
    )
    explanation = explain_anomaly(current)
    alert = _one(
        """
        SELECT alert_id, severity, possible_cause, servicenow_number, sink, created_ts
        FROM ops.alerts
        WHERE cell_id = %s
        ORDER BY created_ts DESC
        LIMIT 1
        """,
        (cell_id,),
    )
    return {
        "cell_id": cell_id,
        "current_metrics": current,
        "recent_history": list(reversed(history)),
        "anomaly": anomaly,
        "explanation": explanation,
        "latest_alert": alert,
        "as_of": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/alerts")
def list_alerts(limit: int = 50) -> list[dict[str, Any]]:
    return _query(
        """
        SELECT alert_id, event_ts, site_id, cell_id, severity, alert_type,
               anomaly_score, possible_cause, servicenow_number, sink, created_ts
        FROM ops.alerts
        ORDER BY created_ts DESC
        LIMIT %s
        """,
        (min(limit, 200),),
    )


@app.get("/incidents")
def list_incidents(limit: int = 50) -> list[dict[str, Any]]:
    return _query(
        """
        SELECT sys_id, number, alert_id, short_description, sink, created_ts
        FROM ops.servicenow_incidents
        ORDER BY created_ts DESC
        LIMIT %s
        """,
        (min(limit, 200),),
    )


@app.get("/anomalies")
def list_anomalies(limit: int = 50) -> list[dict[str, Any]]:
    return _query(
        """
        SELECT cell_id, event_ts, anomaly_score, is_anomaly, model_version
        FROM ml.anomalies
        WHERE is_anomaly
        ORDER BY event_ts DESC
        LIMIT %s
        """,
        (min(limit, 200),),
    )


@app.get("/pipeline/quality")
def pipeline_quality() -> dict[str, Any]:
    row = _one(
        """
        SELECT *
        FROM ops.data_quality_snapshots
        ORDER BY snapshot_ts DESC
        LIMIT 1
        """
    )
    if row is None:
        return {"status": "no_snapshots"}
    return row


@app.get("/metrics")
def recent_metrics(limit: int = 50) -> list[dict[str, Any]]:
    return _query(
        """
        SELECT cell_id, event_ts, site_id, region, technology, latency_ms,
               throughput_mbps, packet_loss_pct, network_status
        FROM clean.network_metrics
        ORDER BY event_ts DESC
        LIMIT %s
        """,
        (min(limit, 200),),
    )
