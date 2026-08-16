from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from common.logging import get_logger
from simulator.topology import build_topology, seed_database

log = get_logger("seed")


def main() -> None:
    sites, cells = build_topology()
    seed_database(sites, cells)
    log.info(
        "topology_seeded",
        extra={"event": "seed", "count": len(cells)},
    )
    print(f"seeded {len(sites)} sites and {len(cells)} cells")


if __name__ == "__main__":
    main()
