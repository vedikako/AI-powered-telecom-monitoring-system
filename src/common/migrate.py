from __future__ import annotations

MIGRATIONS = [
    "ALTER TABLE ops.alerts ADD COLUMN IF NOT EXISTS possible_cause TEXT",
    "ALTER TABLE ops.alerts ADD COLUMN IF NOT EXISTS evidence JSONB",
    "ALTER TABLE ops.alerts ADD COLUMN IF NOT EXISTS recommended_action TEXT",
    "ALTER TABLE ops.alerts ADD COLUMN IF NOT EXISTS servicenow_number TEXT",
    "ALTER TABLE ops.alerts ADD COLUMN IF NOT EXISTS servicenow_sys_id TEXT",
    "ALTER TABLE ops.alerts ADD COLUMN IF NOT EXISTS sink TEXT",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_alerts_cell_event ON ops.alerts (cell_id, event_ts)",
    "CREATE INDEX IF NOT EXISTS idx_alerts_created ON ops.alerts (created_ts DESC)",
    """
    CREATE TABLE IF NOT EXISTS ops.servicenow_incidents (
        sys_id            TEXT PRIMARY KEY,
        number            TEXT NOT NULL,
        alert_id          TEXT REFERENCES ops.alerts (alert_id),
        short_description TEXT,
        payload           JSONB NOT NULL,
        sink              TEXT NOT NULL,
        created_ts        TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
]


def apply_migrations() -> None:
    from common.db import get_conn

    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in MIGRATIONS:
                cur.execute(sql)
