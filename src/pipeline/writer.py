from __future__ import annotations

from collections import deque
from typing import Any

from psycopg2.extras import Json, execute_values

from common.db import get_conn
from common.logging import get_logger
from common.schemas import NetworkMetricEvent, ValidationFailure

log = get_logger("pipeline.writer")

_recent_keys: deque[tuple[str, str]] = deque(maxlen=20000)
_recent_set: set[tuple[str, str]] = set()


def _remember(key: tuple[str, str]) -> bool:
    """Return True if this key was already seen in the local window."""
    if key in _recent_set:
        return True
    if len(_recent_keys) == _recent_keys.maxlen:
        old = _recent_keys.popleft()
        _recent_set.discard(old)
    _recent_keys.append(key)
    _recent_set.add(key)
    return False


def insert_batch(
    valid: list[tuple[NetworkMetricEvent, int | None, int | None, dict[str, Any]]],
    invalid: list[tuple[ValidationFailure, int | None, int | None]],
) -> tuple[int, int, int]:
    """Insert raw+clean+invalid. Returns (raw_inserted, clean_inserted, duplicates)."""
    raw_n = 0
    clean_n = 0
    dup_n = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            if valid:
                raw_rows = [
                    (partition, offset, Json(payload))
                    for _event, partition, offset, payload in valid
                ]
                execute_values(
                    cur,
                    """
                    INSERT INTO raw.network_metrics (kafka_partition, kafka_offset, payload)
                    VALUES %s
                    """,
                    raw_rows,
                )
                raw_n = len(raw_rows)

                clean_rows = []
                for event, _p, _o, _payload in valid:
                    key = (event.cell_id, event.timestamp.isoformat())
                    is_dup = _remember(key)
                    if is_dup:
                        dup_n += 1
                    row = event.to_clean_row()
                    clean_rows.append(
                        (
                            row["cell_id"],
                            row["event_ts"],
                            row["site_id"],
                            row["region"],
                            row["technology"],
                            row["active_users"],
                            row["throughput_mbps"],
                            row["latency_ms"],
                            row["packet_loss_pct"],
                            row["signal_strength_dbm"],
                            row["cpu_usage_pct"],
                            row["memory_usage_pct"],
                            row["network_status"],
                            is_dup,
                        )
                    )
                inserted = execute_values(
                    cur,
                    """
                    INSERT INTO clean.network_metrics (
                        cell_id, event_ts, site_id, region, technology,
                        active_users, throughput_mbps, latency_ms, packet_loss_pct,
                        signal_strength_dbm, cpu_usage_pct, memory_usage_pct,
                        network_status, is_duplicate
                    )
                    VALUES %s
                    ON CONFLICT (cell_id, event_ts) DO NOTHING
                    RETURNING cell_id
                    """,
                    clean_rows,
                    fetch=True,
                )
                clean_n = len(inserted or [])
                conflict_dups = len(clean_rows) - clean_n
                dup_n = max(dup_n, conflict_dups)

            if invalid:
                execute_values(
                    cur,
                    """
                    INSERT INTO ops.invalid_records
                        (error_reason, error_class, payload, kafka_partition, kafka_offset)
                    VALUES %s
                    """,
                    [
                        (
                            f.error_reason,
                            f.error_class,
                            Json(f.payload) if f.payload is not None else None,
                            partition,
                            offset,
                        )
                        for f, partition, offset in invalid
                    ],
                )
    return raw_n, clean_n, dup_n
