from ml.features import rule_force_anomaly


def test_outage_rule_flags_down_status():
    assert rule_force_anomaly({"network_status": "DOWN", "throughput_mbps": 80, "packet_loss_pct": 1})


def test_outage_rule_flags_near_zero_throughput():
    assert rule_force_anomaly({"network_status": "UP", "throughput_mbps": 0.2, "packet_loss_pct": 1})


def test_normal_row_not_forced():
    assert not rule_force_anomaly(
        {"network_status": "UP", "throughput_mbps": 120.0, "packet_loss_pct": 0.4}
    )
