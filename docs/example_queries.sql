-- Interview / demo SQL. Run inside Postgres:
-- docker compose exec postgres psql -U telecom -d telecom -f - < docs/example_queries.sql

-- Highest average latency by cell (last 6 hours)
SELECT cell_id, region, technology, ROUND(AVG(latency_ms)::numeric, 2) AS avg_latency_ms
FROM clean.network_metrics
WHERE event_ts > NOW() - INTERVAL '6 hours'
GROUP BY cell_id, region, technology
ORDER BY avg_latency_ms DESC
LIMIT 10;

-- Highest packet loss by site
SELECT site_id, region, ROUND(AVG(packet_loss_pct)::numeric, 3) AS avg_loss
FROM clean.network_metrics
WHERE event_ts > NOW() - INTERVAL '6 hours'
GROUP BY site_id, region
ORDER BY avg_loss DESC
LIMIT 10;

-- Anomalies by region
SELECT m.region, COUNT(*) AS anomalies
FROM ml.anomalies a
JOIN clean.network_metrics m ON m.cell_id = a.cell_id AND m.event_ts = a.event_ts
WHERE a.is_anomaly
GROUP BY m.region
ORDER BY anomalies DESC;

-- Average throughput by technology
SELECT technology, ROUND(AVG(throughput_mbps)::numeric, 1) AS avg_throughput_mbps
FROM clean.network_metrics
GROUP BY technology
ORDER BY technology;

-- Repeat incidents (ground truth)
SELECT cell_id, incident_type, COUNT(*) AS incidents
FROM ops.injected_incidents
GROUP BY cell_id, incident_type
ORDER BY incidents DESC
LIMIT 15;

-- Users vs latency (simple correlation inputs)
SELECT
    WIDTH_BUCKET(active_users, 0, 1200, 8) AS user_bucket,
    ROUND(AVG(latency_ms)::numeric, 1) AS avg_latency_ms,
    COUNT(*) AS samples
FROM clean.network_metrics
GROUP BY 1
ORDER BY 1;

-- Pipeline quality (latest snapshot)
SELECT *
FROM ops.data_quality_snapshots
ORDER BY snapshot_ts DESC
LIMIT 5;
