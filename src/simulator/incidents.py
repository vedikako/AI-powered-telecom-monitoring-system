from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal
from uuid import uuid4

import numpy as np

from simulator.topology import Cell

IncidentType = Literal["CONGESTION", "HARDWARE", "OUTAGE", "INTERFERENCE"]
Phase = Literal["onset", "peak", "recovery"]

TYPE_WEIGHTS = {
    "CONGESTION": 0.40,
    "INTERFERENCE": 0.25,
    "HARDWARE": 0.20,
    "OUTAGE": 0.15,
}

SEVERITY_BY_TYPE = {
    "CONGESTION": "HIGH",
    "INTERFERENCE": "MEDIUM",
    "HARDWARE": "HIGH",
    "OUTAGE": "CRITICAL",
}


@dataclass
class IncidentEffect:
    incident_type: IncidentType
    intensity: float


@dataclass
class ActiveIncident:
    incident_id: str
    cell_id: str
    incident_type: IncidentType
    severity: str
    start_ts: datetime
    end_ts: datetime
    phase: Phase
    ticks_in_phase: int
    phase_ticks: dict[Phase, int]
    recorded: bool = False
    closed: bool = False

    def intensity(self) -> float:
        if self.phase == "onset":
            return self.ticks_in_phase / max(self.phase_ticks["onset"], 1)
        if self.phase == "peak":
            return 1.0
        remaining = self.phase_ticks["recovery"] - self.ticks_in_phase
        return max(0.0, remaining / max(self.phase_ticks["recovery"], 1))

    def effect(self) -> IncidentEffect:
        return IncidentEffect(self.incident_type, self.intensity())


class IncidentManager:
    def __init__(self, cells: list[Cell], rng: np.random.Generator, interval_seconds: float) -> None:
        self.cells = {c.cell_id: c for c in cells}
        self.by_site: dict[str, list[str]] = {}
        for c in cells:
            self.by_site.setdefault(c.site_id, []).append(c.cell_id)
        self.rng = rng
        self.interval = interval_seconds
        self.active: dict[str, ActiveIncident] = {}
        self.started: list[ActiveIncident] = []
        self.ended: list[ActiveIncident] = []

    def _duration_ticks(self) -> dict[Phase, int]:
        total_min = float(self.rng.uniform(8, 40))
        total_ticks = max(6, int((total_min * 60) / self.interval))
        onset = max(2, int(total_ticks * 0.25))
        recovery = max(2, int(total_ticks * 0.30))
        peak = max(2, total_ticks - onset - recovery)
        return {"onset": onset, "peak": peak, "recovery": recovery}

    def _maybe_start(self, cell: Cell, ts: datetime) -> None:
        if cell.cell_id in self.active:
            return
        base_p = 0.0045 if cell.is_repeat_offender else 0.0016
        hour = ts.hour
        if 8 <= hour <= 10 or 18 <= hour <= 21:
            base_p *= 1.8
        if self.rng.random() > base_p:
            return

        types = list(TYPE_WEIGHTS)
        probs = np.array([TYPE_WEIGHTS[t] for t in types], dtype=float)
        probs /= probs.sum()
        itype: IncidentType = str(self.rng.choice(types, p=probs))  # type: ignore[assignment]
        phases = self._duration_ticks()
        total = sum(phases.values())
        inc = ActiveIncident(
            incident_id=f"INC-{uuid4().hex[:10].upper()}",
            cell_id=cell.cell_id,
            incident_type=itype,
            severity=SEVERITY_BY_TYPE[itype],
            start_ts=ts,
            end_ts=ts + timedelta(seconds=total * self.interval),
            phase="onset",
            ticks_in_phase=0,
            phase_ticks=phases,
        )
        self.active[cell.cell_id] = inc
        self.started.append(inc)

        # Optional site-local congestion spillover.
        if itype == "CONGESTION" and self.rng.random() < 0.35:
            neighbors = [cid for cid in self.by_site[cell.site_id] if cid not in self.active]
            for cid in neighbors[:2]:
                spill = ActiveIncident(
                    incident_id=f"INC-{uuid4().hex[:10].upper()}",
                    cell_id=cid,
                    incident_type="CONGESTION",
                    severity="MEDIUM",
                    start_ts=ts,
                    end_ts=ts + timedelta(seconds=sum(phases.values()) * self.interval),
                    phase="onset",
                    ticks_in_phase=0,
                    phase_ticks=phases,
                )
                self.active[cid] = spill
                self.started.append(spill)

    def _advance(self, inc: ActiveIncident, ts: datetime) -> None:
        inc.ticks_in_phase += 1
        if inc.ticks_in_phase < inc.phase_ticks[inc.phase]:
            return
        inc.ticks_in_phase = 0
        if inc.phase == "onset":
            inc.phase = "peak"
        elif inc.phase == "peak":
            inc.phase = "recovery"
        else:
            inc.end_ts = ts
            inc.closed = True

    def tick(self, ts: datetime) -> dict[str, IncidentEffect]:
        self.started.clear()
        self.ended.clear()
        for cell in self.cells.values():
            self._maybe_start(cell, ts)
        finished: list[str] = []
        for cell_id, inc in self.active.items():
            self._advance(inc, ts)
            if inc.closed:
                finished.append(cell_id)
        for cell_id in finished:
            inc = self.active.pop(cell_id)
            self.ended.append(inc)
        return {cid: inc.effect() for cid, inc in self.active.items()}
