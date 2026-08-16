from __future__ import annotations

import json
from pathlib import Path

from common.config import settings
from common.db import get_conn
from common.logging import get_logger

log = get_logger("ml.evaluate")

EVAL_SQL = """
SELECT
    a.is_anomaly,
    CASE
        WHEN i.incident_id IS NULL THEN 0
        ELSE 1
    END AS is_incident
FROM ml.anomalies a
LEFT JOIN ops.injected_incidents i
    ON i.cell_id = a.cell_id
   AND a.event_ts >= i.start_ts
   AND (i.end_ts IS NULL OR a.event_ts <= i.end_ts)
WHERE a.model_version = %s
"""


def evaluate(model_version: str | None = None) -> dict:
    version = model_version or settings.model_version
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(EVAL_SQL, (version,))
            rows = cur.fetchall()
    if not rows:
        raise RuntimeError("no anomaly rows to evaluate; wait for the ML worker")

    tp = fp = tn = fn = 0
    for is_anomaly, is_incident in rows:
        pred = bool(is_anomaly)
        truth = bool(is_incident)
        if pred and truth:
            tp += 1
        elif pred and not truth:
            fp += 1
        elif not pred and not truth:
            tn += 1
        else:
            fn += 1

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    fpr = fp / (fp + tn) if (fp + tn) else 0.0
    result = {
        "model_version": version,
        "samples": len(rows),
        "true_positives": tp,
        "false_positives": fp,
        "true_negatives": tn,
        "false_negatives": fn,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "false_positive_rate": round(fpr, 4),
        "confusion_matrix": {"tp": tp, "fp": fp, "tn": tn, "fn": fn},
    }
    out = Path(settings.model_path).parent / "eval.json"
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    log.info("evaluation_done", extra={"event": "evaluate", "count": len(rows)})
    return result
