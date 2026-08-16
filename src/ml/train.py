from __future__ import annotations

from pathlib import Path

import joblib
import pandas as pd
from sklearn.ensemble import IsolationForest

from common.config import settings
from common.db import get_conn
from common.logging import get_logger
from ml.features import build_feature_frame, compute_baselines

log = get_logger("ml.train")

TRAIN_SQL = """
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
WHERE NOT EXISTS (
    SELECT 1
    FROM ops.injected_incidents i
    WHERE i.cell_id = m.cell_id
      AND m.event_ts >= i.start_ts
      AND (i.end_ts IS NULL OR m.event_ts <= i.end_ts)
)
"""


def fetch_normal_rows() -> pd.DataFrame:
    with get_conn() as conn:
        df = pd.read_sql(TRAIN_SQL, conn)
    return df


def train_and_save(min_rows: int | None = None) -> dict:
    min_rows = min_rows if min_rows is not None else settings.ml_min_train_rows
    df = fetch_normal_rows()
    if len(df) < min_rows:
        raise RuntimeError(f"not enough normal rows to train: {len(df)} < {min_rows}")
    baselines = compute_baselines(df)
    X = build_feature_frame(df, baselines)
    model = IsolationForest(
        n_estimators=200,
        contamination=0.02,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X)
    bundle = {
        "model": model,
        "baselines": baselines,
        "version": settings.model_version,
        "train_rows": int(len(df)),
    }
    path = Path(settings.model_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, path)
    log.info("model_trained", extra={"event": "train", "count": len(df)})
    return bundle


def load_bundle(path: str | None = None) -> dict:
    return joblib.load(path or settings.model_path)
