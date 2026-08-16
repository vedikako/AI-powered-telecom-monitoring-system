from datetime import datetime, timezone

import numpy as np

from simulator.incidents import IncidentEffect
from simulator.physics import diurnal_factor, generate_event
from simulator.topology import Cell


def _cell() -> Cell:
    return Cell(
        cell_id="CELL_001_01",
        site_id="SITE_001",
        region="Pune",
        technology="5G",
        band="n78",
        max_capacity_users=900,
        site_type="urban",
        baseline_signal_dbm=-72.0,
        is_repeat_offender=False,
    )


def test_evening_load_higher_than_night():
    night = datetime(2026, 8, 17, 3, 0, tzinfo=timezone.utc)  # Monday
    evening = datetime(2026, 8, 17, 19, 30, tzinfo=timezone.utc)
    assert diurnal_factor(evening) > diurnal_factor(night)


def test_congestion_raises_latency_and_drops_throughput():
    cell = _cell()
    ts = datetime(2026, 8, 16, 19, 0, tzinfo=timezone.utc)
    normal = generate_event(cell, ts, np.random.default_rng(1), None)
    congested = generate_event(
        cell, ts, np.random.default_rng(1), IncidentEffect("CONGESTION", 1.0)
    )
    assert congested["latency_ms"] > normal["latency_ms"]
    assert congested["throughput_mbps"] < normal["throughput_mbps"]
    assert congested["packet_loss_pct"] > normal["packet_loss_pct"]
    assert congested["active_users"] >= normal["active_users"]
    assert "incident_type" not in congested


def test_outage_marks_cell_down():
    cell = _cell()
    ts = datetime(2026, 8, 16, 12, 0, tzinfo=timezone.utc)
    event = generate_event(cell, ts, np.random.default_rng(2), IncidentEffect("OUTAGE", 1.0))
    assert event["network_status"] == "DOWN"
    assert event["throughput_mbps"] < 5
    assert event["packet_loss_pct"] > 70


def test_interference_weakens_signal():
    cell = _cell()
    ts = datetime(2026, 8, 16, 12, 0, tzinfo=timezone.utc)
    normal = generate_event(cell, ts, np.random.default_rng(3), None)
    bad = generate_event(
        cell, ts, np.random.default_rng(3), IncidentEffect("INTERFERENCE", 1.0)
    )
    assert bad["signal_strength_dbm"] < normal["signal_strength_dbm"]
    assert bad["throughput_mbps"] < normal["throughput_mbps"]


def test_hardware_raises_cpu_and_memory():
    cell = _cell()
    ts = datetime(2026, 8, 16, 12, 0, tzinfo=timezone.utc)
    normal = generate_event(cell, ts, np.random.default_rng(4), None)
    bad = generate_event(
        cell, ts, np.random.default_rng(4), IncidentEffect("HARDWARE", 1.0)
    )
    assert bad["cpu_usage_pct"] > normal["cpu_usage_pct"]
    assert bad["memory_usage_pct"] > normal["memory_usage_pct"]
