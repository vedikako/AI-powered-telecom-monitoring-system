from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from common.db import get_conn
from common.logging import get_logger

log = get_logger("pipeline.quality")


@dataclass
class QualityTracker:
    received: int = 0
    valid: int = 0
    invalid: int = 0
    duplicate: int = 0
    missing_field: int = 0
    schema_fail: int = 0
    range_fail: int = 0
    db_error: int = 0
    processing_latency_ms: float = 0.0
    last_flush: float = field(default_factory=time.time)
    window_received: int = 0

    def mark_received(self, n: int = 1) -> None:
        self.received += n
        self.window_received += n

    def mark_valid(self, n: int = 1) -> None:
        self.valid += n

    def mark_duplicate(self, n: int = 1) -> None:
        self.duplicate += n

    def mark_invalid(self, error_class: str, n: int = 1) -> None:
        self.invalid += n
        if error_class == "missing_field":
            self.missing_field += n
        elif error_class == "range_fail":
            self.range_fail += n
        else:
            self.schema_fail += n

    def mark_db_error(self, n: int = 1) -> None:
        self.db_error += n

    def set_latency(self, ms: float) -> None:
        self.processing_latency_ms = ms

    def maybe_flush(self, interval_s: float, consumer_lag: int | None) -> None:
        now = time.time()
        if now - self.last_flush < interval_s:
            return
        elapsed = max(now - self.last_flush, 1e-6)
        rps = self.window_received / elapsed
        freshness = _freshness_seconds()
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ops.data_quality_snapshots (
                        snapshot_ts, records_received, records_valid, records_invalid,
                        records_duplicate, missing_field, schema_fail, range_fail,
                        db_error, consumer_lag, freshness_seconds, records_per_sec,
                        processing_latency_ms
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        datetime.now(timezone.utc),
                        self.received,
                        self.valid,
                        self.invalid,
                        self.duplicate,
                        self.missing_field,
                        self.schema_fail,
                        self.range_fail,
                        self.db_error,
                        consumer_lag,
                        freshness,
                        rps,
                        self.processing_latency_ms,
                    ),
                )
        log.info(
            "quality_snapshot",
            extra={
                "event": "quality",
                "count": self.received,
                "lag": consumer_lag,
            },
        )
        self.window_received = 0
        self.last_flush = now


def _freshness_seconds() -> float | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(event_ts) FROM clean.network_metrics")
            row = cur.fetchone()
    if not row or row[0] is None:
        return None
    latest = row[0]
    if latest.tzinfo is None:
        from datetime import timezone as tz

        latest = latest.replace(tzinfo=tz.utc)
    return (datetime.now(timezone.utc) - latest).total_seconds()
