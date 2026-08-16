from __future__ import annotations

from typing import Any


ACTIONS = {
    "OUTAGE": "Check power, backhaul, and neighboring-cell handover. Dispatch field ops if the cell stays DOWN.",
    "CONGESTION": "Investigate cell capacity and neighboring-cell load. Consider traffic steering or additional carriers.",
    "HARDWARE": "Inspect baseband/RRU CPU and memory. Plan a restart or hardware replacement if resource saturation persists.",
    "INTERFERENCE": "Review RF: overlapping PCI/PCI, external interference, and antenna tilt. Check RSRP vs neighbouring cells.",
    "MULTIVARIATE_ANOMALY": "Review the metric bundle against this cell's baseline. Confirm whether load, RF, or hardware is the driver.",
}


def explain(metrics: dict[str, Any], baseline: dict[str, Any] | None = None) -> dict[str, Any]:
    """Rule-based cause. No LLM. Safe to use even if the optional LLM wrapper is removed."""
    baseline = baseline or {}
    status = str(metrics.get("network_status") or "UP")
    loss = float(metrics.get("packet_loss_pct") or 0)
    latency = float(metrics.get("latency_ms") or 0)
    throughput = float(metrics.get("throughput_mbps") or 0)
    users = float(metrics.get("active_users") or 0)
    signal = float(metrics.get("signal_strength_dbm") or -80)
    cpu = float(metrics.get("cpu_usage_pct") or 0)
    memory = float(metrics.get("memory_usage_pct") or 0)
    capacity = float(metrics.get("max_capacity_users") or 1)
    utilization = users / max(capacity, 1)

    base_users = float(baseline.get("active_users") or users)
    base_thr = float(baseline.get("throughput_mbps") or max(throughput, 1))

    evidence: list[str] = []
    cause = "MULTIVARIATE_ANOMALY"

    if status == "DOWN" or throughput < 1.0 or loss >= 50:
        cause = "OUTAGE"
        evidence = [
            f"network_status={status}",
            f"throughput={throughput:.1f} Mbps",
            f"packet_loss={loss:.1f}%",
        ]
    elif cpu >= 80 and memory >= 70:
        cause = "HARDWARE"
        evidence = [
            f"cpu={cpu:.1f}%",
            f"memory={memory:.1f}%",
            f"throughput={throughput:.1f} Mbps",
            f"latency={latency:.1f} ms",
        ]
    elif signal <= -100 and loss >= 3:
        cause = "INTERFERENCE"
        evidence = [
            f"signal={signal:.1f} dBm",
            f"packet_loss={loss:.1f}%",
            f"throughput={throughput:.1f} Mbps",
        ]
    elif utilization >= 0.75 and throughput < base_thr * 0.75 and latency >= 50:
        cause = "CONGESTION"
        evidence = [
            f"active_users={int(users)} (utilization={utilization:.0%})",
            f"throughput={throughput:.1f} Mbps (baseline ~{base_thr:.1f})",
            f"latency={latency:.1f} ms",
            f"packet_loss={loss:.1f}%",
        ]
    else:
        evidence = [
            f"latency={latency:.1f} ms",
            f"packet_loss={loss:.1f}%",
            f"throughput={throughput:.1f} Mbps",
            f"active_users={int(users)}",
            f"signal={signal:.1f} dBm",
        ]

    return {
        "possible_cause": cause,
        "evidence": evidence,
        "recommended_action": ACTIONS[cause],
        "source": "rules",
    }
