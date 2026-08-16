# Real-Time Telecom Network Analytics Platform

Student-scale **data engineering** project: synthetic 4G/5G cell telemetry is streamed through Kafka, validated, stored in PostgreSQL, visualized in Grafana, and scored with a small Isolation Forest model.

This is **not** a RAG/chatbot project. Isolation Forest is a supporting component.

## Architecture

```
Simulator  →  Kafka (network.metrics)
                 │
                 ├─ invalid → network.metrics.dlq + ops.invalid_records
                 ▼
            Processor (validate, batch insert)
                 │
                 ▼
            PostgreSQL
              raw / clean / analytics / ml / ops
                 │
        ┌────────┼────────┐
        ▼        ▼        ▼
     Grafana   Batch    Isolation Forest
     (NOC +    ETL      (per-cell baselines)
     pipeline)
```

Kafka messages are keyed by `cell_id` (ordering per cell). Incident labels are **not** on the wire; they are written only to `ops.injected_incidents` for evaluation.

See [docs/architecture.md](docs/architecture.md) for schema, topics, and interview talking points.

## Stack

Python • Kafka • PostgreSQL • Grafana • Isolation Forest • Docker Compose

## Quick start

Requirements: Docker Desktop.

If this repo lives under OneDrive, prefer copying it to a local disk (for example `C:\src\telecom-network-analytics`) before `compose up`. OneDrive file locking can corrupt Kafka/Postgres volumes.

```bash
cp .env.example .env   # optional; compose has defaults
docker compose up --build
```

Wait a few minutes for:

1. Topology seed (20 sites / 100 cells)
2. Historical backfill (~6 hours at 10s cadence)
3. Live streaming
4. ML worker training once enough clean rows exist (~40k)

Open Grafana: [http://localhost:3000](http://localhost:3000)

- User / password: `admin` / `admin` (anonymous Viewer is also enabled)
- Dashboards folder **Telecom**:
  - **Network Operations** — latency, throughput, loss, users, anomalies
  - **Pipeline Health** — validity, lag, freshness, invalid records

## What runs

| Service | Role |
|---|---|
| `kafka` | KRaft broker (no ZooKeeper). Topics: `network.metrics` (6 partitions), `network.metrics.dlq` (3) |
| `postgres` | Layered warehouse: `raw`, `clean`, `analytics`, `ml`, `ops` |
| `seed` | Inserts sites/cells |
| `simulator` | Physics-based telemetry + incident state machine + Kafka producer |
| `processor` | Consumer group `metrics-processor`; validate; batch write; commit offsets after DB success |
| `batch` | Hourly cell + daily site SQL rollups (loop, not Airflow) |
| `ml-worker` | Train Isolation Forest on normal windows; score unscored rows |
| `grafana` | Provisioned dashboards |

## Useful commands

```bash
docker compose ps
docker compose logs -f processor
docker compose logs -f simulator
docker compose exec ml-worker python /app/scripts/evaluate_model.py
docker compose exec processor python /app/scripts/replay_dlq.py
docker compose exec postgres psql -U telecom -d telecom -c "SELECT COUNT(*) FROM clean.network_metrics;"
```

Unit tests (no Docker):

```bash
pip install -r requirements.txt
pytest
```

## Data

- 5 regions: Pune, Mumbai, Hyderabad, Bengaluru, Chennai
- 20 sites, 100 cells, mix of 4G/5G
- Interval: 10 seconds
- Metrics are **derived** (load → users → utilization → latency/throughput/loss/CPU), not independent random draws
- Injected scenarios: congestion, hardware degradation, outage, interference
- ~1.5% of events are intentionally invalid (negative latency, loss > 100, missing `cell_id`, bad timestamp) so the error path is visible

## ML

Isolation Forest uses current metrics plus **per-cell z-scores**, utilization, and short deltas. Outages are also forced by a simple rule (`DOWN` / near-zero throughput / loss > 50%).

Do not invent accuracy numbers. After the worker has scored data:

```bash
docker compose exec ml-worker python /app/scripts/evaluate_model.py
```

That prints precision, recall, F1, false-positive rate, and a confusion matrix against `ops.injected_incidents`.

## Measured results

From a local Docker run on 16 Aug 2026 after the consumer caught up (Kafka lag = 0 on all 6 partitions).

| Area | Metric | Value |
|---|---|---|
| Pipeline | Events processed | 232,204 valid rows in `clean.network_metrics`; 3,596 invalid |
| Pipeline | Invalid record % | 1.52% (3,596 / 235,800) — matches the ~1.5% injected defects |
| Pipeline | Consumer lag (steady state) | 0 (live in-flight ~0–50) |
| Pipeline | Freshness (seconds) | ~5 s |
| Pipeline | Validate latency | 0.01–0.14 ms / event |
| ML | Precision / recall / F1 | 0.58 / 1.00 / 0.73 (n=232,301; FPR=0.23) |

The Isolation Forest was trained on an early backfill window, so recall is high but peak-hour load is over-flagged. Retraining on a full-day mix of normal cells would be the next ML improvement — not a fake 0.99 F1.

**Where each number comes from (not the Grafana KPI row):**

```bash
# Pipeline quality (received, valid, invalid, lag, freshness)
docker exec telecom-postgres psql -U telecom -d telecom -c "SELECT * FROM ops.data_quality_snapshots ORDER BY snapshot_ts DESC LIMIT 1;"

# ML precision / recall / F1 vs injected incidents
docker exec telecom-ml python /app/scripts/evaluate_model.py
```

Grafana **Network Operations** is network health (cells, latency, throughput). Grafana **Pipeline Health** is closer to this table (lag, invalid count, freshness).

## Post-MVP (in this repo)

These sit **on top of** the pipeline. Removing them does not stop Kafka, Postgres, Grafana, or Isolation Forest.

| Piece | What it does | Default |
|---|---|---|
| Alert engine | Turns recent anomalies into `ops.alerts` with severity + rule-based cause | On (`alert-engine` service) |
| ServiceNow sink | `AlertSink` protocol: mock locally, real Table API if credentials are set | **Mock** (free, no account) |
| FastAPI | Cell analysis, alerts, incidents, pipeline quality | [http://localhost:8000/docs](http://localhost:8000/docs) |
| Explanation | Rules first (`CONGESTION` / `OUTAGE` / `HARDWARE` / `INTERFERENCE`). Optional LLM sentence if `LLM_ENABLED=true` | Rules only |

```bash
# After compose is up
curl http://localhost:8000/health
curl http://localhost:8000/network-health
curl http://localhost:8000/alerts
curl http://localhost:8000/incidents
curl http://localhost:8000/cells/CELL_001_01/analysis
```

HIGH/CRITICAL alerts create a ServiceNow incident via the mock sink (`ops.servicenow_incidents`, numbers like `INC0000001`). To use a real developer instance, set `SERVICENOW_INSTANCE`, `SERVICENOW_USER`, and `SERVICENOW_PASSWORD` in `.env`. To drop the LLM later, leave `LLM_ENABLED=false` or delete `src/explain/llm.py` — `explain.rules` still works.

## Project identity

Primary: **data engineering / streaming / analytics**  
Secondary: small unsupervised anomaly detection evaluated against hidden labels
