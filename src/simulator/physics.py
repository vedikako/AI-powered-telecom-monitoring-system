from __future__ import annotations

import math
from datetime import datetime
from typing import Any

import numpy as np

from simulator.incidents import IncidentEffect
from simulator.topology import Cell


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def diurnal_factor(ts: datetime) -> float:
    """Two daily peaks (morning commute, evening) with a quiet night."""
    hour = ts.hour + ts.minute / 60.0 + ts.second / 3600.0
    weekend = ts.weekday() >= 5
    morning_mu = 10.5 if weekend else 9.0
    evening_mu = 20.0 if weekend else 19.5
    morning = math.exp(-0.5 * ((hour - morning_mu) / 1.3) ** 2)
    evening = math.exp(-0.5 * ((hour - evening_mu) / 1.6) ** 2)
    base = 0.16 if weekend else 0.18
    peak = 0.72 if weekend else 0.84
    return clamp(base + peak * (0.85 * morning + evening), 0.14, 1.08)


def _tech_ceilings(cell: Cell) -> tuple[float, float]:
    """Return (max_throughput_mbps, base_latency_ms)."""
    if cell.technology == "5G":
        thr = 420.0 if cell.site_type == "urban" else 280.0 if cell.site_type == "suburban" else 180.0
        lat = 18.0
    else:
        thr = 95.0 if cell.site_type == "urban" else 70.0 if cell.site_type == "suburban" else 45.0
        lat = 32.0
    return thr, lat


def generate_event(
    cell: Cell,
    ts: datetime,
    rng: np.random.Generator,
    incident: IncidentEffect | None = None,
) -> dict[str, Any]:
    """Derive correlated metrics from cell state. Labels are never included."""
    load = diurnal_factor(ts)
    if cell.site_type == "highway":
        load *= 0.75 + 0.35 * math.sin((ts.hour / 24.0) * math.pi)
    load = clamp(load * rng.normal(1.0, 0.04), 0.08, 1.15)

    users = int(clamp(load * cell.max_capacity_users * rng.normal(1.0, 0.03), 5, cell.max_capacity_users * 1.25))
    utilization = users / float(cell.max_capacity_users)

    signal = cell.baseline_signal_dbm + float(rng.normal(0, 1.8))
    max_thr, base_lat = _tech_ceilings(cell)
    signal_factor = clamp((signal + 120) / 50.0, 0.35, 1.15)

    # Queueing: throughput falls and latency/loss rise as the cell fills.
    congestion = utilization**2
    throughput = max_thr * (1.0 - 0.55 * congestion) * signal_factor * rng.normal(1.0, 0.03)
    latency = base_lat * (1.0 + 2.4 * congestion) / max(signal_factor, 0.4) * rng.normal(1.0, 0.04)
    packet_loss = 0.04 + 3.2 * (utilization**3) + max(0.0, (-95 - signal) * 0.08)
    cpu = 18 + 62 * utilization + float(rng.normal(0, 2.5))
    memory = 28 + 45 * utilization + float(rng.normal(0, 2.0))
    status = "UP"

    if incident is not None:
        k = incident.intensity
        if incident.incident_type == "CONGESTION":
            users = int(clamp(users * (1.35 + 0.55 * k), users, cell.max_capacity_users * 1.45))
            utilization = users / float(cell.max_capacity_users)
            throughput *= 1.0 - 0.55 * k
            latency *= 1.8 + 2.2 * k
            packet_loss += 4.0 + 12.0 * k
            cpu = clamp(cpu + 12 * k, 0, 99)
        elif incident.incident_type == "HARDWARE":
            cpu = clamp(78 + 20 * k + float(rng.normal(0, 2)), 70, 100)
            memory = clamp(72 + 22 * k + float(rng.normal(0, 2)), 65, 100)
            throughput *= 1.0 - 0.45 * k
            latency *= 1.5 + 1.4 * k
            packet_loss += 1.5 + 4.0 * k
        elif incident.incident_type == "OUTAGE":
            throughput = abs(float(rng.normal(0.4, 0.3)))
            latency *= 4.0 + 6.0 * k
            packet_loss = clamp(85 + 12 * k + float(rng.normal(0, 2)), 70, 100)
            users = max(0, int(users * (1.0 - 0.85 * k)))
            status = "DOWN"
        elif incident.incident_type == "INTERFERENCE":
            signal -= 12 + 18 * k
            throughput *= 1.0 - 0.5 * k
            packet_loss += 3.5 + 10.0 * k
            latency *= 1.25 + 0.9 * k

    event = {
        "timestamp": ts.isoformat(),
        "site_id": cell.site_id,
        "cell_id": cell.cell_id,
        "region": cell.region,
        "technology": cell.technology,
        "active_users": int(users),
        "throughput_mbps": round(clamp(throughput, 0.0, 800.0), 2),
        "latency_ms": round(clamp(latency, 1.0, 2000.0), 2),
        "packet_loss_pct": round(clamp(packet_loss, 0.0, 100.0), 3),
        "signal_strength_dbm": round(clamp(signal, -140.0, -30.0), 1),
        "cpu_usage_pct": round(clamp(cpu, 1.0, 100.0), 2),
        "memory_usage_pct": round(clamp(memory, 1.0, 100.0), 2),
        "network_status": status,
    }
    return event


def corrupt_event(event: dict[str, Any], rng: np.random.Generator) -> dict[str, Any]:
    """Inject a data-quality defect for the DLQ path. Does not drop the record."""
    bad = dict(event)
    kind = int(rng.integers(0, 5))
    if kind == 0:
        bad["latency_ms"] = -12.5
    elif kind == 1:
        bad["packet_loss_pct"] = 140.0
    elif kind == 2:
        bad.pop("cell_id", None)
    elif kind == 3:
        bad["timestamp"] = "not-a-timestamp"
    else:
        bad["cpu_usage_pct"] = 250.0
    return bad
