from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from pydantic import ValidationError

from common.config import settings
from common.schemas import NetworkMetricEvent, ValidationFailure


def _classify(error: Exception | str) -> str:
    text = str(error).lower()
    if "required" in text or "missing" in text or "field required" in text:
        return "missing_field"
    if "must be" in text or "between" in text or "percentage" in text or "signal" in text:
        return "range_fail"
    return "schema_fail"


def validate_payload(
    raw: bytes | str | dict[str, Any],
    now: datetime | None = None,
) -> tuple[NetworkMetricEvent | None, ValidationFailure | None]:
    now = now or datetime.now(timezone.utc)
    if isinstance(raw, (bytes, bytearray)):
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            return None, ValidationFailure(
                error_class="schema_fail",
                error_reason=f"invalid json: {exc}",
                payload=None,
            )
    elif isinstance(raw, str):
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            return None, ValidationFailure(
                error_class="schema_fail",
                error_reason=f"invalid json: {exc}",
                payload=None,
            )
    else:
        payload = raw

    if not isinstance(payload, dict):
        return None, ValidationFailure(
            error_class="schema_fail",
            error_reason="payload is not an object",
            payload=None,
        )

    required = (
        "timestamp",
        "site_id",
        "cell_id",
        "region",
        "technology",
        "active_users",
        "throughput_mbps",
        "latency_ms",
        "packet_loss_pct",
        "signal_strength_dbm",
        "cpu_usage_pct",
        "memory_usage_pct",
        "network_status",
    )
    missing = [f for f in required if f not in payload or payload[f] is None]
    if missing:
        return None, ValidationFailure(
            error_class="missing_field",
            error_reason=f"missing fields: {', '.join(missing)}",
            payload=payload,
        )

    try:
        event = NetworkMetricEvent.model_validate(payload)
    except ValidationError as exc:
        return None, ValidationFailure(
            error_class=_classify(exc),
            error_reason=str(exc.errors()[0].get("msg", exc)),
            payload=payload,
        )

    event_ts = event.timestamp
    if event_ts.tzinfo is None:
        event_ts = event_ts.replace(tzinfo=timezone.utc)
        event.timestamp = event_ts

    if event_ts > now + timedelta(minutes=5):
        return None, ValidationFailure(
            error_class="range_fail",
            error_reason="timestamp is in the future",
            payload=payload,
        )

    age = (now - event_ts).total_seconds()
    max_age = max(settings.max_event_age_seconds, settings.backfill_hours * 3600 + 3600)
    if age > max_age:
        return None, ValidationFailure(
            error_class="range_fail",
            error_reason=f"timestamp too old ({age:.0f}s)",
            payload=payload,
        )
    return event, None
