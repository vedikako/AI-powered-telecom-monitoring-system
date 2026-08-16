-- Layered warehouse in one PostgreSQL instance:
--   ops        dimensions, ground truth, quality, reserved alerts
--   raw        append-only Kafka payloads
--   clean      validated, typed telemetry
--   analytics  hourly/daily rollups
--   ml         anomaly scores

CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS clean;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS ml;

CREATE TABLE ops.sites (
    site_id     TEXT PRIMARY KEY,
    region      TEXT NOT NULL,
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    site_type   TEXT NOT NULL CHECK (site_type IN ('urban', 'suburban', 'highway'))
);

CREATE TABLE ops.cells (
    cell_id              TEXT PRIMARY KEY,
    site_id              TEXT NOT NULL REFERENCES ops.sites (site_id),
    technology           TEXT NOT NULL CHECK (technology IN ('4G', '5G')),
    band                 TEXT NOT NULL,
    max_capacity_users   INTEGER NOT NULL CHECK (max_capacity_users > 0),
    is_repeat_offender   BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE raw.network_metrics (
    id               BIGSERIAL PRIMARY KEY,
    ingest_ts        TIMESTAMPTZ NOT NULL DEFAULT now(),
    kafka_partition  INTEGER,
    kafka_offset     BIGINT,
    payload          JSONB NOT NULL
);

CREATE TABLE clean.network_metrics (
    cell_id              TEXT NOT NULL REFERENCES ops.cells (cell_id),
    event_ts             TIMESTAMPTZ NOT NULL,
    site_id              TEXT NOT NULL REFERENCES ops.sites (site_id),
    region               TEXT NOT NULL,
    technology           TEXT NOT NULL,
    active_users         INTEGER NOT NULL,
    throughput_mbps      DOUBLE PRECISION NOT NULL,
    latency_ms           DOUBLE PRECISION NOT NULL,
    packet_loss_pct      DOUBLE PRECISION NOT NULL,
    signal_strength_dbm  DOUBLE PRECISION NOT NULL,
    cpu_usage_pct        DOUBLE PRECISION NOT NULL,
    memory_usage_pct     DOUBLE PRECISION NOT NULL,
    network_status       TEXT NOT NULL CHECK (network_status IN ('UP', 'DOWN')),
    is_duplicate         BOOLEAN NOT NULL DEFAULT FALSE,
    processed_ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (cell_id, event_ts)
);

CREATE TABLE analytics.hourly_cell_metrics (
    cell_id               TEXT NOT NULL REFERENCES ops.cells (cell_id),
    hour_ts               TIMESTAMPTZ NOT NULL,
    site_id               TEXT NOT NULL,
    region                TEXT NOT NULL,
    technology            TEXT NOT NULL,
    avg_latency_ms        DOUBLE PRECISION,
    p95_latency_ms        DOUBLE PRECISION,
    avg_throughput_mbps   DOUBLE PRECISION,
    avg_packet_loss_pct   DOUBLE PRECISION,
    max_active_users      INTEGER,
    avg_cpu_usage_pct     DOUBLE PRECISION,
    sample_count          INTEGER,
    anomaly_count         INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (cell_id, hour_ts)
);

CREATE TABLE analytics.daily_site_metrics (
    site_id               TEXT NOT NULL REFERENCES ops.sites (site_id),
    day_ts                DATE NOT NULL,
    region                TEXT NOT NULL,
    avg_latency_ms        DOUBLE PRECISION,
    avg_throughput_mbps   DOUBLE PRECISION,
    avg_packet_loss_pct   DOUBLE PRECISION,
    max_active_users      INTEGER,
    outage_minutes        DOUBLE PRECISION,
    anomaly_count         INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (site_id, day_ts)
);

-- Ground truth for injected incidents. Never placed on the Kafka payload.
CREATE TABLE ops.injected_incidents (
    incident_id    TEXT PRIMARY KEY,
    cell_id        TEXT NOT NULL REFERENCES ops.cells (cell_id),
    incident_type  TEXT NOT NULL CHECK (
        incident_type IN ('CONGESTION', 'HARDWARE', 'OUTAGE', 'INTERFERENCE')
    ),
    start_ts       TIMESTAMPTZ NOT NULL,
    end_ts         TIMESTAMPTZ,
    severity       TEXT NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    created_ts     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ops.data_quality_snapshots (
    snapshot_ts            TIMESTAMPTZ PRIMARY KEY DEFAULT now(),
    records_received       BIGINT NOT NULL DEFAULT 0,
    records_valid          BIGINT NOT NULL DEFAULT 0,
    records_invalid        BIGINT NOT NULL DEFAULT 0,
    records_duplicate      BIGINT NOT NULL DEFAULT 0,
    missing_field          BIGINT NOT NULL DEFAULT 0,
    schema_fail            BIGINT NOT NULL DEFAULT 0,
    range_fail             BIGINT NOT NULL DEFAULT 0,
    db_error               BIGINT NOT NULL DEFAULT 0,
    consumer_lag           BIGINT,
    freshness_seconds      DOUBLE PRECISION,
    records_per_sec        DOUBLE PRECISION,
    processing_latency_ms  DOUBLE PRECISION
);

CREATE TABLE ml.anomalies (
    cell_id         TEXT NOT NULL,
    event_ts        TIMESTAMPTZ NOT NULL,
    anomaly_score   DOUBLE PRECISION NOT NULL,
    is_anomaly      BOOLEAN NOT NULL,
    model_version   TEXT NOT NULL,
    detected_ts     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (cell_id, event_ts, model_version)
);

-- Reserved for post-MVP alerting / ServiceNow. Unused in MVP.
CREATE TABLE ops.alerts (
    alert_id        TEXT PRIMARY KEY,
    event_ts        TIMESTAMPTZ NOT NULL,
    site_id         TEXT,
    cell_id         TEXT,
    severity        TEXT NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    alert_type      TEXT,
    metrics         JSONB,
    anomaly_score   DOUBLE PRECISION,
    created_ts      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ops.invalid_records (
    id               BIGSERIAL PRIMARY KEY,
    ingest_ts        TIMESTAMPTZ NOT NULL DEFAULT now(),
    error_reason     TEXT NOT NULL,
    error_class      TEXT NOT NULL,
    payload          JSONB,
    kafka_partition  INTEGER,
    kafka_offset     BIGINT
);

CREATE INDEX idx_clean_metrics_event_ts
    ON clean.network_metrics (event_ts DESC);

CREATE INDEX idx_clean_metrics_cell_ts
    ON clean.network_metrics (cell_id, event_ts DESC);

CREATE INDEX idx_clean_metrics_region_tech
    ON clean.network_metrics (region, technology, event_ts DESC);

CREATE INDEX idx_raw_ingest_ts
    ON raw.network_metrics (ingest_ts DESC);

CREATE INDEX idx_incidents_cell_window
    ON ops.injected_incidents (cell_id, start_ts, end_ts);

CREATE INDEX idx_anomalies_ts
    ON ml.anomalies (event_ts DESC);

CREATE INDEX idx_anomalies_flag
    ON ml.anomalies (is_anomaly, event_ts DESC);

CREATE INDEX idx_hourly_hour_ts
    ON analytics.hourly_cell_metrics (hour_ts DESC);

CREATE INDEX idx_quality_snapshot_ts
    ON ops.data_quality_snapshots (snapshot_ts DESC);
