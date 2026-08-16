from __future__ import annotations

import pandas as pd
from psycopg2.extras import execute_values

from common.config import settings
from common.db import get_conn
from common.logging import get_logger
from ml.features import build_feature_frame, rule_force_anomaly
from ml.train import load_bundle

log = get_logger("ml.infer")

UNSCORED_SQL = """
WITH sequenced AS (
    SELECT
        m.cell_id,
        m.event_ts,
        m.latency_ms,
        m.packet_loss_pct,
        m.throughput_mbps,
        m.active_users,
        m.signal_strength_dbm,
        m.cpu_usage_pct,
        m.memory_usage_pct,
        m.network_status,
        c.max_capacity_users,
        LAG(m.latency_ms) OVER (PARTITION BY m.cell_id ORDER BY m.event_ts) AS prev_latency,
        LAG(m.throughput_mbps) OVER (PARTITION BY m.cell_id ORDER BY m.event_ts) AS prev_throughput
    FROM clean.network_metrics m
    JOIN ops.cells c ON c.cell_id = m.cell_id
)
SELECT s.*
FROM sequenced s
LEFT JOIN ml.anomalies a
    ON a.cell_id = s.cell_id
   AND a.event_ts = s.event_ts
   AND a.model_version = %s
WHERE a.cell_id IS NULL
ORDER BY s.event_ts
LIMIT %s
"""


def score_unscored(bundle: dict | None = None, batch_size: int = 8000) -> int:
    bundle = bundle or load_bundle()
    model = bundle["model"]
    baselines = bundle["baselines"]
    version = bundle["version"]
    with get_conn() as conn:
        df = pd.read_sql(UNSCORED_SQL, conn, params=(version, batch_size))
    if df.empty:
        return 0
    df = df.reset_index(drop=True)
    X = build_feature_frame(df, baselines)
    preds = model.predict(X)
    scores = model.decision_function(X)
    rows = []
    for i, row in df.iterrows():
        is_anom = bool(preds[i] == -1) or rule_force_anomaly(row)
        score = float(scores[i])
        if rule_force_anomaly(row):
            score = min(score, -0.25)
        rows.append((row["cell_id"], row["event_ts"], score, is_anom, version))
    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO ml.anomalies (cell_id, event_ts, anomaly_score, is_anomaly, model_version)
                VALUES %s
                ON CONFLICT (cell_id, event_ts, model_version) DO NOTHING
                """,
                rows,
            )
    log.info("scored_batch", extra={"event": "infer", "count": len(rows)})
    return len(rows)
