from __future__ import annotations

from dataclasses import dataclass

REGIONS = {
    "Pune": (18.5204, 73.8567),
    "Mumbai": (19.0760, 72.8777),
    "Hyderabad": (17.3850, 78.4867),
    "Bengaluru": (12.9716, 77.5946),
    "Chennai": (13.0827, 80.2707),
}

SITE_TYPES = ("urban", "suburban", "highway")
BANDS_4G = ("1800", "2300")
BANDS_5G = ("3500", "n78")


@dataclass(frozen=True)
class Site:
    site_id: str
    region: str
    latitude: float
    longitude: float
    site_type: str


@dataclass(frozen=True)
class Cell:
    cell_id: str
    site_id: str
    region: str
    technology: str
    band: str
    max_capacity_users: int
    site_type: str
    baseline_signal_dbm: float
    is_repeat_offender: bool


def build_topology(seed: int = 42) -> tuple[list[Site], list[Cell]]:
    """20 sites x 5 cells = 100 cells across 5 regions."""
    import random

    rng = random.Random(seed)
    sites: list[Site] = []
    cells: list[Cell] = []
    site_idx = 1
    repeat_ids: set[str] = set()

    for region, (lat0, lon0) in REGIONS.items():
        for n in range(4):
            site_type = SITE_TYPES[(site_idx + n) % len(SITE_TYPES)]
            site_id = f"SITE_{site_idx:03d}"
            lat = lat0 + rng.uniform(-0.08, 0.08)
            lon = lon0 + rng.uniform(-0.08, 0.08)
            sites.append(Site(site_id, region, lat, lon, site_type))

            for c in range(1, 6):
                cell_id = f"CELL_{site_idx:03d}_{c:02d}"
                technology = "5G" if (c + site_idx) % 3 != 0 else "4G"
                if technology == "5G":
                    band = BANDS_5G[(c + site_idx) % len(BANDS_5G)]
                    capacity = 900 if site_type == "urban" else 650 if site_type == "suburban" else 400
                    signal = -72.0 if site_type == "urban" else -78.0
                else:
                    band = BANDS_4G[(c + site_idx) % len(BANDS_4G)]
                    capacity = 550 if site_type == "urban" else 400 if site_type == "suburban" else 280
                    signal = -82.0 if site_type == "urban" else -88.0
                if site_type == "highway":
                    signal -= 6
                cells.append(
                    Cell(
                        cell_id=cell_id,
                        site_id=site_id,
                        region=region,
                        technology=technology,
                        band=band,
                        max_capacity_users=capacity,
                        site_type=site_type,
                        baseline_signal_dbm=signal,
                        is_repeat_offender=False,
                    )
                )
            site_idx += 1

    # Mark 8 cells as repeat offenders (higher incident rate).
    candidates = [c.cell_id for c in cells if c.site_type in {"urban", "highway"}]
    rng.shuffle(candidates)
    repeat_ids = set(candidates[:8])
    cells = [
        Cell(
            cell_id=c.cell_id,
            site_id=c.site_id,
            region=c.region,
            technology=c.technology,
            band=c.band,
            max_capacity_users=c.max_capacity_users,
            site_type=c.site_type,
            baseline_signal_dbm=c.baseline_signal_dbm,
            is_repeat_offender=c.cell_id in repeat_ids,
        )
        for c in cells
    ]
    return sites, cells


def seed_database(sites: list[Site], cells: list[Cell]) -> None:
    from common.db import get_conn, execute_values

    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO ops.sites (site_id, region, latitude, longitude, site_type)
                VALUES %s
                ON CONFLICT (site_id) DO UPDATE SET
                    region = EXCLUDED.region,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    site_type = EXCLUDED.site_type
                """,
                [(s.site_id, s.region, s.latitude, s.longitude, s.site_type) for s in sites],
            )
            execute_values(
                cur,
                """
                INSERT INTO ops.cells (
                    cell_id, site_id, technology, band, max_capacity_users, is_repeat_offender
                )
                VALUES %s
                ON CONFLICT (cell_id) DO UPDATE SET
                    site_id = EXCLUDED.site_id,
                    technology = EXCLUDED.technology,
                    band = EXCLUDED.band,
                    max_capacity_users = EXCLUDED.max_capacity_users,
                    is_repeat_offender = EXCLUDED.is_repeat_offender
                """,
                [
                    (
                        c.cell_id,
                        c.site_id,
                        c.technology,
                        c.band,
                        c.max_capacity_users,
                        c.is_repeat_offender,
                    )
                    for c in cells
                ],
            )


def load_cells_from_db() -> list[Cell]:
    from common.db import get_conn

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.cell_id, c.site_id, s.region, c.technology, c.band,
                       c.max_capacity_users, s.site_type, c.is_repeat_offender
                FROM ops.cells c
                JOIN ops.sites s ON s.site_id = c.site_id
                ORDER BY c.cell_id
                """
            )
            rows = cur.fetchall()

    # Reconstruct baseline from site_type/technology (deterministic).
    out: list[Cell] = []
    for row in rows:
        cell_id, site_id, region, tech, band, cap, site_type, repeat = row
        if tech == "5G":
            signal = -72.0 if site_type == "urban" else -78.0
        else:
            signal = -82.0 if site_type == "urban" else -88.0
        if site_type == "highway":
            signal -= 6
        out.append(
            Cell(
                cell_id=cell_id,
                site_id=site_id,
                region=region,
                technology=tech,
                band=band,
                max_capacity_users=cap,
                site_type=site_type,
                baseline_signal_dbm=signal,
                is_repeat_offender=repeat,
            )
        )
    return out
