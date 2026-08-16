from alerts.severity import classify_severity, should_open_incident
from explain.rules import explain


def test_outage_is_critical():
    sev = classify_severity(
        {"network_status": "DOWN", "packet_loss_pct": 90, "latency_ms": 400, "throughput_mbps": 0.2},
        is_anomaly=True,
    )
    assert sev == "CRITICAL"
    assert should_open_incident(sev)


def test_congestion_style_is_high_or_medium():
    sev = classify_severity(
        {"network_status": "UP", "packet_loss_pct": 12, "latency_ms": 180, "throughput_mbps": 40},
        is_anomaly=True,
    )
    assert sev == "HIGH"


def test_non_anomaly_raises_nothing():
    assert classify_severity({"network_status": "UP", "packet_loss_pct": 90}, is_anomaly=False) is None


def test_rules_detect_outage():
    result = explain({"network_status": "DOWN", "throughput_mbps": 0.3, "packet_loss_pct": 88})
    assert result["possible_cause"] == "OUTAGE"
    assert result["source"] == "rules"


def test_rules_detect_congestion():
    result = explain(
        {
            "network_status": "UP",
            "active_users": 800,
            "max_capacity_users": 900,
            "throughput_mbps": 40,
            "latency_ms": 90,
            "packet_loss_pct": 4,
            "signal_strength_dbm": -80,
            "cpu_usage_pct": 60,
            "memory_usage_pct": 50,
        },
        baseline={"throughput_mbps": 180, "active_users": 400},
    )
    assert result["possible_cause"] == "CONGESTION"
    assert "Investigate cell capacity" in result["recommended_action"]


def test_rules_detect_hardware():
    result = explain(
        {
            "network_status": "UP",
            "cpu_usage_pct": 92,
            "memory_usage_pct": 88,
            "throughput_mbps": 50,
            "latency_ms": 70,
            "packet_loss_pct": 2,
            "active_users": 200,
            "signal_strength_dbm": -78,
        }
    )
    assert result["possible_cause"] == "HARDWARE"
