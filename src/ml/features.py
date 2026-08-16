from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

RAW_FEATURES = [
    "latency_ms",
    "packet_loss_pct",
    "throughput_mbps",
    "active_users",
    "signal_strength_dbm",
    "cpu_usage_pct",
    "memory_usage_pct",
]

FEATURE_COLUMNS = RAW_FEATURES + [
    "utilization",
    "z_latency_ms",
    "z_packet_loss_pct",
    "z_throughput_mbps",
    "z_active_users",
    "z_signal_strength_dbm",
    "z_cpu_usage_pct",
    "z_memory_usage_pct",
    "d_latency_ms",
    "d_throughput_mbps",
]


@dataclass
class CellBaselines:
    means: dict[str, dict[str, float]]
    stds: dict[str, dict[str, float]]


def compute_baselines(df: pd.DataFrame) -> CellBaselines:
    means: dict[str, dict[str, float]] = {}
    stds: dict[str, dict[str, float]] = {}
    for cell_id, grp in df.groupby("cell_id"):
        means[cell_id] = {col: float(grp[col].mean()) for col in RAW_FEATURES}
        stds[cell_id] = {
            col: float(grp[col].std(ddof=0) or 1.0) for col in RAW_FEATURES
        }
    return CellBaselines(means=means, stds=stds)


def build_feature_frame(df: pd.DataFrame, baselines: CellBaselines) -> pd.DataFrame:
    out = df.copy()
    out["utilization"] = out["active_users"] / out["max_capacity_users"].clip(lower=1)
    out["d_latency_ms"] = out["latency_ms"] - out["prev_latency"].fillna(out["latency_ms"])
    out["d_throughput_mbps"] = out["throughput_mbps"] - out["prev_throughput"].fillna(
        out["throughput_mbps"]
    )
    global_means = {col: float(out[col].mean()) for col in RAW_FEATURES}
    global_stds = {col: float(out[col].std(ddof=0) or 1.0) for col in RAW_FEATURES}
    for col in RAW_FEATURES:
        means = out["cell_id"].map(
            lambda cid, c=col: baselines.means.get(cid, {}).get(c, global_means[c])
        ).astype(float)
        stds = out["cell_id"].map(
            lambda cid, c=col: (baselines.stds.get(cid, {}).get(c, global_stds[c]) or 1.0)
        ).astype(float)
        out[f"z_{col}"] = (out[col].astype(float) - means) / stds.replace(0, 1.0)
    return out[FEATURE_COLUMNS].replace([np.inf, -np.inf], np.nan).fillna(0.0)


def rule_force_anomaly(row) -> bool:
    return bool(
        row.get("network_status") == "DOWN"
        or float(row.get("throughput_mbps", 1)) < 1.0
        or float(row.get("packet_loss_pct", 0)) > 50.0
    )
