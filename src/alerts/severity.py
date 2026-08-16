from __future__ import annotations

from typing import Any


def classify_severity(metrics: dict[str, Any], is_anomaly: bool) -> str | None:
    """Return severity or None if no alert should be raised."""
    if not is_anomaly:
        return None
    status = str(metrics.get("network_status") or "UP")
    loss = float(metrics.get("packet_loss_pct") or 0)
    latency = float(metrics.get("latency_ms") or 0)
    throughput = float(metrics.get("throughput_mbps") or 0)

    if status == "DOWN" or throughput < 1.0 or loss >= 50:
        return "CRITICAL"
    if loss >= 10 and latency >= 150:
        return "HIGH"
    if loss >= 3 or latency >= 80:
        return "MEDIUM"
    return "LOW"


def should_open_incident(severity: str) -> bool:
    return severity in {"HIGH", "CRITICAL"}
