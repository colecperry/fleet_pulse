# Flink SQL Streaming Job

## What is this?

`job.sql` is a Flink SQL job that sits between Kafka and PostgreSQL. It reads raw telemetry events from the `telemetry` Kafka topic, computes 30-second rolling aggregations per vehicle, and writes the results to the `vehicle_aggregates` table in PostgreSQL.

The REST API reads from `vehicle_aggregates` directly — cheap indexed lookups instead of scanning millions of raw events on every request.

---

## Concepts

### Tumbling windows

A tumbling window is a fixed-size, non-overlapping time bucket. Every 30 seconds Flink closes the current window, emits the aggregation, and opens a new one. No event belongs to more than one window.

```
t=0s ──────── t=30s ──────── t=60s ──────── t=90s
 [  window 1  ] [  window 2  ] [  window 3  ]
```

A sliding window overlaps (e.g. 30s window advancing every 5s), counting each event in multiple windows. More expensive and not what we want — we need discrete 30-second snapshots per vehicle.

### Event time vs processing time

- Processing time — uses Flink's machine clock. A delayed event lands in whatever window is currently open, not where it belongs.
- Event time — uses recorded_at from the event itself. A delayed event lands in the correct historical window.
- For fleet telemetry, event time is correct — a late GPS ping should count in the window it actually happened, not when

### Watermarks

Flink can't wait forever for late events. The watermark strategy says: "assume all events up to `(max_seen_event_time - 5s)` have arrived." Events more than 5 seconds late are dropped.

---

## Setup

### 1. Download connector JARs

Flink's base image doesn't include Kafka or JDBC connectors. Run once from the project root:

```bash
bash flink/download_jars.sh
```

This saves three JARs into `flink/jars/` which are mounted into the Flink containers automatically via `docker-compose.yml`.

### 2. Start the stack

```bash
docker-compose down && docker-compose up -d
```

### 3. Submit the job

```bash
docker exec -it fleet_pulse-flink-jobmanager-1 /opt/flink/bin/sql-client.sh -f /job.sql
```

The SQL client reads `job.sql` (mounted at `/job.sql` in the container), creates the source and sink tables, and starts the streaming job.

---

## Monitoring

Open the Flink dashboard at [http://localhost:8081](http://localhost:8081):

- **Running Jobs** — confirm the job is active
- **Task Managers** — see event throughput per second  
- **Checkpoints** — Flink snapshots state periodically so the job can resume after a restart without reprocessing everything

---

## What gets written to PostgreSQL

For each vehicle, every 30 seconds:

|      Column      |               Description 
|------------------|----------------------------------------------
| `vehicle_id`     | Which vehicle 
| `avg_speed_kmph` | Average speed across all events in the window 
| `event_count`    | Number of telemetry events received 
| `window_start`   | When the 30s window opened 
| `window_end`     | When the 30s window closed 
