from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


class NetworkMetricEvent(BaseModel):
    timestamp: datetime
    site_id: str = Field(min_length=1)
    cell_id: str = Field(min_length=1)
    region: str = Field(min_length=1)
    technology: str
    active_users: int
    throughput_mbps: float
    latency_ms: float
    packet_loss_pct: float
    signal_strength_dbm: float
    cpu_usage_pct: float
    memory_usage_pct: float
    network_status: str

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_be_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    @field_validator("technology")
    @classmethod
    def technology_ok(cls, value: str) -> str:
        if value not in {"4G", "5G"}:
            raise ValueError("technology must be 4G or 5G")
        return value

    @field_validator("network_status")
    @classmethod
    def status_ok(cls, value: str) -> str:
        if value not in {"UP", "DOWN"}:
            raise ValueError("network_status must be UP or DOWN")
        return value

    @field_validator("active_users")
    @classmethod
    def users_ok(cls, value: int) -> int:
        if value < 0:
            raise ValueError("active_users must be >= 0")
        return value

    @field_validator("throughput_mbps", "latency_ms")
    @classmethod
    def non_negative(cls, value: float) -> float:
        if value < 0:
            raise ValueError("metric must be >= 0")
        return value

    @field_validator("packet_loss_pct", "cpu_usage_pct", "memory_usage_pct")
    @classmethod
    def pct_range(cls, value: float) -> float:
        if value < 0 or value > 100:
            raise ValueError("percentage must be between 0 and 100")
        return value

    @field_validator("signal_strength_dbm")
    @classmethod
    def signal_range(cls, value: float) -> float:
        if value > 0 or value < -140:
            raise ValueError("signal_strength_dbm must be between -140 and 0")
        return value

    def to_clean_row(self) -> dict[str, Any]:
        return {
            "cell_id": self.cell_id,
            "event_ts": self.timestamp,
            "site_id": self.site_id,
            "region": self.region,
            "technology": self.technology,
            "active_users": self.active_users,
            "throughput_mbps": self.throughput_mbps,
            "latency_ms": self.latency_ms,
            "packet_loss_pct": self.packet_loss_pct,
            "signal_strength_dbm": self.signal_strength_dbm,
            "cpu_usage_pct": self.cpu_usage_pct,
            "memory_usage_pct": self.memory_usage_pct,
            "network_status": self.network_status,
        }


class ValidationFailure(BaseModel):
    error_class: str
    error_reason: str
    payload: Optional[dict[str, Any]] = None
