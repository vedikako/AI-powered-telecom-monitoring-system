from datetime import datetime, timezone

from pipeline.validator import validate_payload


NOW = datetime(2026, 8, 16, 12, 0, tzinfo=timezone.utc)

VALID = {
    "timestamp": "2026-08-16T11:59:00+00:00",
    "site_id": "SITE_001",
    "cell_id": "CELL_001_01",
    "region": "Pune",
    "technology": "5G",
    "active_users": 120,
    "throughput_mbps": 180.4,
    "latency_ms": 28.1,
    "packet_loss_pct": 0.4,
    "signal_strength_dbm": -78.0,
    "cpu_usage_pct": 41.2,
    "memory_usage_pct": 50.0,
    "network_status": "UP",
}


def test_valid_event_passes():
    event, failure = validate_payload(VALID, now=NOW)
    assert failure is None
    assert event is not None
    assert event.cell_id == "CELL_001_01"


def test_negative_latency_rejected():
    payload = dict(VALID, latency_ms=-12)
    event, failure = validate_payload(payload, now=NOW)
    assert event is None
    assert failure is not None
    assert failure.error_class == "range_fail"


def test_packet_loss_over_100_rejected():
    payload = dict(VALID, packet_loss_pct=140)
    event, failure = validate_payload(payload, now=NOW)
    assert event is None
    assert failure.error_class == "range_fail"


def test_missing_cell_id_rejected():
    payload = dict(VALID)
    del payload["cell_id"]
    event, failure = validate_payload(payload, now=NOW)
    assert event is None
    assert failure.error_class == "missing_field"


def test_bad_json_rejected():
    event, failure = validate_payload(b"not-json", now=NOW)
    assert event is None
    assert failure.error_class == "schema_fail"


def test_future_timestamp_rejected():
    payload = dict(VALID, timestamp="2026-08-16T18:00:00+00:00")
    event, failure = validate_payload(payload, now=NOW)
    assert event is None
    assert failure.error_class == "range_fail"
