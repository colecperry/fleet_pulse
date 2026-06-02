# Fleet Pulse

A real-time fleet telemetry pipeline using TypeScript, Node.js, Kafka, Flink, PostgreSQL, and Docker Compose — extended with a Model Context Protocol (MCP) server that exposes live vehicle data and pipeline health to Claude as a set of queryable tools.

---

## Architecture

```
                              
                                       ┌───────PostgreSQL──────┐
                    ┌─────Flink─────────▶ vehicle_aggregates   │
                    │                  │                       │
Simulator ──▶ Kafka                    │                       │
                    │                  │                       │
                    └────Consumer───────▶ telemetry_events ──────▶ REST API
                                       └───────────────────────┘      │
                                                                      ▼
                                                                 MCP Server
                                                                      │
                                                                      ▼
                                                               Claude Desktop
```   

---

## Prerequisites

- Node.js 20+
- Docker and Docker Compose
- Claude Desktop (for MCP integration)

---

## Getting Started

### 1. Clone and install
```bash
git clone https://github.com/your-username/fleet-pulse.git
cd fleet-pulse
npm install
cp .env.example .env
```

### 2. Download Flink connector JARs (one-time)
```bash
bash flink/download_jars.sh
```

### 3. Start infrastructure
```bash
docker-compose up -d
```

Verify Kafka, Postgres, and Flink are running:
- API health: http://localhost:3000/health
- Flink dashboard: http://localhost:8081
- Kafdrop (Kafka UI): http://localhost:9000

### 4. Create database tables (One time, re-run if schema changes)
```bash
npm run migrate
```

### 5. Start the API server
```bash
npm run dev
```

### 6. Start the vehicle simulator
```bash
npm run simulate
```

You should see 5 vehicles publishing events every second in the terminal.

### 7. Set up Flink connectors (one-time)

Download the connector JARs before starting the stack:

```bash
bash flink/download_jars.sh
```

> docker-compose mounts each JAR directly into `/opt/flink/lib/` inside the Flink containers. The JARs must exist locally before `docker-compose up` runs.

### 8. Submit the Flink job
```bash
docker exec -it fleet_pulse-flink-jobmanager-1 /opt/flink/bin/sql-client.sh -f /job.sql
```

The job runs continuously — every 30 seconds it writes one aggregation row per vehicle to `vehicle_aggregates`. Monitor it at http://localhost:8081.

### 8. Verify data is flowing

```bash
# Check raw events
psql postgres://fleet:fleet@localhost:5433/fleet_pulse -c "SELECT * FROM telemetry_events LIMIT 5;"

# Check Flink aggregations (populated after 30s)
psql postgres://fleet:fleet@localhost:5433/fleet_pulse -c "SELECT * FROM vehicle_aggregates LIMIT 5;"
```

Or browse Kafka messages at http://localhost:9000 — click the `telemetry` topic → View Messages.

### Viewing Kafka events (Kafdrop)

Kafdrop is a web UI for browsing Kafka topics and reading individual messages.

```bash
# Start the simulator in one terminal
npm run simulate

# Open Kafdrop in your browser
open http://localhost:9000
```

In Kafdrop:
1. Click the **`telemetry`** topic
2. Click **View Messages**
3. Set offset to `0`, click **View Messages**

You'll see the raw JSON payload for each vehicle event:

```json
{
  "vehicle_id": "VH-003",
  "lat": 37.7741,
  "lon": -122.4209,
  "speed_kmph": 34.12,
  "status": "moving",
  "recorded_at": "2026-06-01T21:05:00.000Z"
}
```

### Viewing PostgreSQL tables

Connect with psql using the Docker postgres (port 5433):

```bash
psql postgres://fleet:fleet@localhost:5433/fleet_pulse
```

```sql
SELECT * FROM telemetry_events LIMIT 10;
SELECT * FROM vehicle_aggregates LIMIT 10;
```

Or connect via **TablePlus** or **pgAdmin** with these credentials:

| Field    | Value         |
|----------|---------------|
| Host     | `localhost`   |
| Port     | `5433`        |
| User     | `fleet`       |
| Password | `fleet`       |
| Database | `fleet_pulse` |

> **Note:** `telemetry_events` will be empty until the Kafka consumer is running (`npm run consume`).

### Stopping infrastructure

```bash
# Stop containers but keep data
docker-compose down

# Stop containers and wipe the database volume
docker-compose down -v
```

---

## Project Structure

```
fleet-pulse/
├── src/
│   ├── index.ts          # Express entry point
│   ├── db.ts             # Postgres connection pool + query helper
│   ├── schema.sql        # Table definitions (run once via migrate.ts)
│   ├── migrate.ts        # One-shot script: applies schema.sql to the DB
│   ├── simulator.ts      # Publishes fake vehicle telemetry to Kafka on a loop
│   ├── producer.ts       # Shared Kafka producer — used by simulator and POST /telemetry
│   └── routes/
│       └── telemetry.ts  # POST /telemetry — validates and publishes events to Kafka
├── flink/
│   ├── job.sql           # Flink SQL job: Kafka → 30s tumbling windows → vehicle_aggregates
│   ├── download_jars.sh  # Downloads Kafka + JDBC connector JARs
│   ├── jars/             # Connector JARs (not committed — run download_jars.sh)
│   └── README.md         # Flink concepts and submission instructions
├── .env                  # Local secrets (not committed)
├── .env.example          # Committed template with placeholder values
├── .gitignore
├── docker-compose.yml    # Local infrastructure (Kafka, Postgres, Flink)
├── package.json
├── tsconfig.json
└── README.md
```

---

## npm Scripts

| Script          | Command                               | Purpose                       |
|-----------------|---------------------------------------|-------------------------------|
| `npm run dev`     | `nodemon --exec ts-node src/index.ts` | Start server with live reload      |
| `npm run build`   | `tsc`                                 | Compile TypeScript to `dist/`      |
| `npm start`       | `node dist/index.js`                  | Run compiled production build      |
| `npm run migrate` | `ts-node src/migrate.ts`              | Apply schema.sql to the database   |
| `npm run simulate`| `ts-node src/simulator.ts`            | Start the vehicle simulator        |

---

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `PORT` | HTTP server port | `3000` |
| `DATABASE_URL` | PostgreSQL connection string | — |
| `TEST_DATABASE_URL` | Separate DB for integration tests | — |
| `KAFKA_BROKER` | Kafka broker address (`host:port`) | — |
| `SIMULATOR_INTERVAL_MS` | How often the simulator ticks per vehicle | `1000` |
| `LOG_LEVEL` | Pino log level (`info`, `debug`, etc.) | `info` |

---

## Steps Completed

- [x] Step 1 — Project scaffold (TypeScript, Express, dotenv, nodemon)
- [x] Step 2 — Docker Compose infrastructure (Kafka, Zookeeper, PostgreSQL, Flink)
- [x] Step 3 — PostgreSQL schema and connection pool
- [x] Step 4 — Vehicle simulator
- [x] Step 5 — Kafka producer and POST /telemetry route
- [x] Step 6 — Flink stream processing job
- [ ] Step 7 — Kafka consumer
- [ ] Step 8 — REST API endpoints
- [ ] Step 9 — Structured logging (Pino)
- [ ] Step 10 — Tests (Jest + Supertest)
- [ ] Step 11 — CI/CD (GitHub Actions + Docker)
- [ ] Step 12 — MCP server
- [ ] Step 13 — Full README
