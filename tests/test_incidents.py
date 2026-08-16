from simulator.incidents import IncidentManager
from simulator.topology import build_topology
import numpy as np


def test_topology_scale():
    sites, cells = build_topology()
    assert len(sites) == 20
    assert len(cells) == 100
    assert {s.region for s in sites} == {
        "Pune",
        "Mumbai",
        "Hyderabad",
        "Bengaluru",
        "Chennai",
    }
    assert any(c.technology == "4G" for c in cells)
    assert any(c.technology == "5G" for c in cells)
    assert sum(1 for c in cells if c.is_repeat_offender) == 8


def test_incident_durations_have_all_phases():
    _sites, cells = build_topology()
    manager = IncidentManager(cells[:3], np.random.default_rng(0), interval_seconds=10)
    for _ in range(20):
        phases = manager._duration_ticks()
        assert phases["onset"] >= 2
        assert phases["peak"] >= 2
        assert phases["recovery"] >= 2
        assert sum(phases.values()) >= 6
