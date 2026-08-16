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

Fill these after a real run. Leave blank until then.

| Area | Metric | Value |
|---|---|---|
| Pipeline | Events processed | _TBD_ |
| Pipeline | Invalid record % | _TBD_ |
| Pipeline | Consumer lag (steady state) | _TBD_ |
| Pipeline | Freshness (seconds) | _TBD_ |
| ML | Precision / recall / F1 | _TBD_ |

## Post-MVP (not in this repo yet)

Alert engine → ServiceNow incident create, FastAPI cell analysis, rule/LLM explanation that can be removed without breaking the pipeline.

## Project identity

Primary: **data engineering / streaming / analytics**  
Secondary: small unsupervised anomaly detection evaluated against hidden labels
