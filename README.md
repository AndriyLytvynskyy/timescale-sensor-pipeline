# Real-Time Sensor Telemetry Pipeline

An end-to-end, Dockerized pipeline that simulates IoT sensors, streams their readings
through Kafka, stores them in **TimescaleDB**, and exposes two kinds of views:

- a **latest-state view** — the most recent reading per sensor, and
- an **aggregated view** — rolling 1-minute and 1-hour statistics (avg / min / max / stddev / count).

Both views are served to a lightweight read **query API** and pre-provisioned **Grafana** dashboards.

![Grafana sensor dashboard](docs/grafana-dashboard.png)

---

## Architecture

```
                       ┌──────────────────────┐
                       │   sensor-simulator    │   10 virtual sensors
                       │   (random walk, 1 Hz) │   temp · humidity · pressure · CO₂ · vibration
                       └───────────┬──────────┘
                                   │ HTTP POST /events
                                   ▼
                       ┌──────────────────────┐
                       │  ingestion-service    │   FastAPI · :8000
                       │  validate (Pydantic)  │   POST /events, /events/batch
                       └───────────┬──────────┘
                                   │ kafka-python producer (acks=all)
                                   ▼
                       ┌──────────────────────┐
                       │        Kafka          │   KRaft mode (no Zookeeper)
                       │  topic: sensor-events │   internal :9092 · external :9094
                       └───────────┬──────────┘
                                   │ kafka-python consumer (manual offset commit)
                                   ▼
                       ┌──────────────────────┐
                       │    kafka-consumer     │   batch insert (execute_values)
                       │  at-least-once + dedup│   raw events only — no hot write path
                       └───────────┬──────────┘
                                   │ psycopg2 bulk insert
                                   ▼
        ┌────────────────────────────────────────────────────────┐
        │                      TimescaleDB                         │   PostgreSQL 15 + Timescale
        │                                                          │
        │   WRITE (batch insert)          READ (computed on read)  │
        │   ┌──────────────┐              ┌──────────────────────┐ │
        │   │ sensor_events│──── feeds ──▶│ sensor_latest   VIEW  │ │  DISTINCT ON + SkipScan
        │   │  (hypertable)│              │ sensor_stats_1min CA  │ │  1-min continuous agg
        │   │ raw, by time │              │ sensor_stats_1hour CA │ │  1-hour hierarchical CA
        │   └──────────────┘              └──────────────────────┘ │
        │                                                          │
        │   no latest-state table · no upsert hot path             │
        │   compression > 7d · retention 90d                       │
        └───────────────┬─────────────────────────┬───────────────┘
                        │                          │
                        ▼                          ▼
             ┌───────────────────┐     ┌────────────────────┐
             │   query-service   │     │      Grafana        │
             │  FastAPI · :8001  │     │  dashboards · :3000 │
             │  read-only, open  │     │  single datasource  │
             └───────────────────┘     └────────────────────┘

     ┌──────────┐
     │ kafka-ui │  broker/topic inspection · :8080
     └──────────┘
```

---

## Components

| Component            | Tech                     | Port   | Role                                                                 |
|----------------------|--------------------------|--------|---------------------------------------------------------------------|
| `sensor-simulator`   | Python + httpx           | –      | 10 virtual sensors doing a bounded random walk, one round per `SEND_INTERVAL`. |
| `ingestion-service`  | FastAPI                  | 8000   | Validates events with Pydantic, publishes to the Kafka topic.       |
| `kafka`              | apache/kafka (KRaft)     | 9094   | Durable event buffer between ingestion and storage. No Zookeeper.   |
| `kafka-ui`           | provectus kafka-ui       | 8080   | Web UI to inspect brokers, topics, and messages.                    |
| `kafka-consumer`     | Python + psycopg2        | –      | Batches messages and bulk-inserts raw events into the hypertable.   |
| `timescaledb`        | TimescaleDB (PG15)       | 5433   | Time-series storage, continuous aggregates, compression, retention. |
| `query-service`      | FastAPI                  | 8001   | Open read API over the latest-state view and aggregates.            |
| `grafana`            | Grafana OSS              | 3000   | Pre-provisioned datasource + sensors dashboard.                     |

---

## Data model (TimescaleDB)

| Object                | Type                       | Purpose                                                                 |
|-----------------------|----------------------------|-------------------------------------------------------------------------|
| `sensor_events`       | hypertable                 | Raw readings, partitioned by `time`. Source of truth.                   |
| `sensor_latest`       | **view**                   | **Latest state** — one row per sensor via `DISTINCT ON (sensor_id)`, computed on read. |
| `sensor_stats_1min`   | continuous aggregate       | **Aggregated view** — 1-min buckets, refreshed every minute.            |
| `sensor_stats_1hour`  | continuous aggregate       | 1-hour buckets, built hierarchically on the 1-min CA, refreshed every 30 min. |

**Lifecycle policies:** raw chunks compress after **7 days**; raw data is dropped after **90 days**.

> Note: because `sensor_stats_1hour` aggregates from `sensor_stats_1min`, hourly data can
> lag raw events by up to ~31 minutes (1-min refresh + 30-min hourly refresh). It is not real-time.

### Latest state: a view, not a table

`sensor_latest` is a **view**, computed at read time:

```sql
CREATE OR REPLACE VIEW sensor_latest AS
SELECT DISTINCT ON (sensor_id) sensor_id, sensor_type, location,
       time AS last_seen, value, unit, quality
FROM sensor_events
ORDER BY sensor_id, time DESC;
```

Backed by the `(sensor_id, time DESC)` index, TimescaleDB's **SkipScan** jumps straight to the
newest row per sensor. There is **no per-event upsert**, so the consumer only does a raw batch
insert — ingestion has no hot write path to contend on.

---

## Delivery guarantees

The consumer runs with `enable_auto_commit=False` and only commits Kafka offsets **after** a
successful DB flush. Inserts use `ON CONFLICT DO NOTHING`. Together this gives **at-least-once**
delivery with **no duplicate rows**. If you change one side, update the other.

---

## Quick start

```bash
# optional: override defaults
cp .env.example .env

# start the full stack (builds images on first run)
make up          # == docker compose up --build -d

make ps          # status of all services
make logs        # follow all logs
make down        # stop (keep data volumes)
make clean       # stop + wipe all data volumes
make shell-db    # psql session inside TimescaleDB
```

First boot takes a minute or two while Kafka, TimescaleDB, and Grafana pass their health checks.

---

## Endpoints & UIs

| Service        | URL / connection                                        |
|----------------|---------------------------------------------------------|
| Ingestion API  | http://localhost:8000  (docs at `/docs`)                |
| Query API      | http://localhost:8001  (docs at `/docs`)                |
| Kafka UI       | http://localhost:8080                                   |
| Grafana        | http://localhost:3000  (admin / admin)                  |
| TimescaleDB    | `postgresql://postgres:password@localhost:5433/sensors` |

### Query API

The read API is open (no auth) — it is an internal, single-tenant service.

```bash
# latest reading per sensor
curl -s http://localhost:8001/v1/sensors/latest

# aggregated current state by type
curl -s http://localhost:8001/v1/sensors/summary

# sensors silent beyond a threshold
curl -s http://localhost:8001/v1/sensors/alerts

# 1-minute history for one sensor
curl -s "http://localhost:8001/v1/sensors/temp-bldg-a-01/history"
```

| Endpoint                            | Returns                                               |
|-------------------------------------|-------------------------------------------------------|
| `GET  /v1/sensors/latest`           | Latest reading per sensor (filters: type, location).  |
| `GET  /v1/sensors/summary`          | Aggregated current state grouped by sensor type.      |
| `GET  /v1/sensors/alerts`           | Sensors silent beyond a threshold.                    |
| `GET  /v1/sensors/{id}/history`     | 1-min aggregates over a time range (cursor paging).   |

---

## Configuration

Copy `.env.example` → `.env` to override. Defaults in parentheses.

| Variable            | Default         | Description                                   |
|---------------------|-----------------|-----------------------------------------------|
| `POSTGRES_USER`     | `postgres`      | DB superuser.                                 |
| `POSTGRES_PASSWORD` | `password`      | DB password.                                  |
| `POSTGRES_DB`       | `sensors`       | Database name.                                |
| `KAFKA_TOPIC`       | `sensor-events` | Topic shared across services.                 |
| `SEND_INTERVAL`     | `1.0`           | Seconds between simulator rounds.             |
| `BATCH_SIZE`        | `100`           | Consumer flushes after this many messages.    |
| `BATCH_TIMEOUT`     | `2.0`           | Consumer flushes after this many idle seconds.|

---

## Project layout

```
.
├── docker-compose.yml          # all services
├── Makefile                    # up / down / clean / logs / ps / shell-db
├── .env.example                # configuration template
├── db/
│   └── init.sql                # hypertable, continuous aggregates, sensor_latest view
├── ingestion-service/          # FastAPI → Kafka
├── kafka-consumer/             # Kafka → TimescaleDB (raw batch insert)
├── sensor-simulator/           # virtual sensors
├── query-service/              # read API (open)
└── grafana/
    └── provisioning/           # datasource + sensors dashboard
```
