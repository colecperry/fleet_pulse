# Fleet Pulse

A real-time fleet telemetry pipeline using TypeScript, Node.js, Kafka, Flink, PostgreSQL, and Docker Compose — extended with a Model Context Protocol (MCP) server that exposes live vehicle data and pipeline health to Claude as a set of queryable tools.

---

## Architecture

```
                                                  ┌───────PostgreSQL──────┐
                              ┌─────Flink───────────▶ vehicle_aggregates  │
                              │                   │                       │
Simulator ──▶ POST /telemetry ────▶ Kafka         │                       │
                              │                   │                       │
                              └────Consumer─────────▶ telemetry_events ─────▶ REST API
                                                  └───────────────────────┘      │
                                                                                 ▼
                                                                            MCP Server
                                                                                 │
                                                                                 ▼
                                                                          Claude Desktop
```   

1. The simulator sends an HTTP POST to /telemetry — it doesn't know about Kafka or Postgres, it just speaks HTTP to one endpoint
2. The Express route validates the payload and publishes it to Kafka — the route's only job is to be the entry point and reject bad data early
3. From Kafka, two things happen in parallel:
- Flink reads and writes 30-second aggregations to vehicle_aggregates
- The consumer reads and writes raw events to telemetry_events
4. Both tables feed the REST API (4 GET ENDPOINTS), which feeds the MCP Server, which lets Claude answer questions about the fleet in plain English

---

## Prerequisites

- Node.js 20+
- Docker and Docker Compose
- Claude Desktop (for MCP integration)

---

## Getting Started

### 1. Clone and install
```bash
git clone https://github.com/colecperry/fleet_pulse.git
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

### 4. Create database tables (One time, re-run if schema changes or to wipe data)
```bash
npm run migrate
```

### 5. Start the API server, simulator, and consumer

Run all three processes in one terminal with:

```bash
npm run start:all
```

This uses `concurrently` to run `npm run dev`, `npm run consume`, and `npm run simulate` in parallel, with each process color-coded in the same terminal output.

> The simulator POSTs to `POST /telemetry` which requires the API server to be up. If the simulator logs a connection error on the first tick, it will recover automatically once the server is ready.

Or run each in a separate terminal if you want isolated output:

```bash
npm run dev       # terminal 1
npm run consume   # terminal 2
npm run simulate  # terminal 3
```

Verify raw events are being written to PostgreSQL:
```bash
psql postgres://fleet:fleet@localhost:5433/fleet_pulse -c "SELECT vehicle_id, speed_kmph, status, recorded_at FROM telemetry_events ORDER BY recorded_at DESC LIMIT 5;"
```

### 6. Submit the Flink job
```bash
docker exec -it fleet_pulse-flink-jobmanager-1 /opt/flink/bin/sql-client.sh -f /job.sql
```

The job runs continuously — every 30 seconds it writes one aggregation row per vehicle to `vehicle_aggregates`. Monitor it at http://localhost:8081.

### 7. Verify data is flowing

```bash
# Check Flink aggregations (populated after 30s)
psql postgres://fleet:fleet@localhost:5433/fleet_pulse -c "SELECT * FROM vehicle_aggregates LIMIT 5;"
```

Or browse Kafka messages at http://localhost:9000 — click the `telemetry` topic → View Messages.

### Viewing Kafka events (Kafdrop)

Kafdrop is a web UI for browsing Kafka topics and reading individual messages.

```bash
# Start the pipeline (API + consumer + simulator)
npm run start:all

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
│   ├── simulator.ts      # Simulates 5 vehicles POSTing telemetry to POST /telemetry on a loop
│   ├── producer.ts       # Kafka producer — called by POST /telemetry to publish events
│   ├── consumer.ts       # Reads from Kafka, writes raw events to telemetry_events
│   ├── logger.ts         # Pino logger — pretty in dev, raw JSON in production
│   ├── __tests__/
│   │   ├── setup.ts                      # Swaps DATABASE_URL to TEST_DATABASE_URL before tests run
│   │   ├── telemetry.unit.test.ts        # Validates POST /telemetry with mocked Kafka + DB
│   │   └── telemetry.integration.test.ts # Tests GET endpoints against a real test database
│   └── routes/
│       └── telemetry.ts  # POST /telemetry + four GET endpoints for vehicles and fleet stats
├── mcp/
│   ├── src/
│   │   └── index.ts      # MCP server — 6 tools exposing pipeline data to Claude
│   ├── package.json
│   ├── tsconfig.json
│   └── README.md         # Setup instructions and example queries for Claude Desktop
├── flink/
│   ├── job.sql           # Flink SQL job: Kafka → 30s tumbling windows → vehicle_aggregates
│   ├── download_jars.sh  # Downloads Kafka + JDBC connector JARs
│   ├── jars/             # Connector JARs (not committed — run download_jars.sh)
│   └── README.md         # Flink concepts and submission instructions
├── .env                  # Local secrets (not committed)
├── .env.example          # Committed template with placeholder values
├── .gitignore
├── .dockerignore         # Excludes secrets, test code, and build artifacts from Docker image
├── Dockerfile            # Multi-stage production build (builder → production)
├── docker-compose.yml    # Local infrastructure (Kafka, Postgres, Flink)
├── package.json
├── tsconfig.json
└── README.md
```

---

## npm Scripts

| Script                    | Command                              | Purpose                       
|---------------------------|--------------------------------------|------------------------------------------------
| `npm run dev`             | `nodemon --exec ts-node src/index.ts`| Start server with live reload      
| `npm run build`           | `tsc`                                | Compile TypeScript to `dist/`      
| `npm start`               | `node dist/index.js`                 | Run compiled production build      
| `npm run migrate`         | `ts-node src/migrate.ts`             | Apply schema.sql to the database   
| `npm run simulate`        | `ts-node src/simulator.ts`           | Start the vehicle simulator (requires `npm run dev`) 
| `npm run consume`         | `ts-node src/consumer.ts`            | Start the Kafka consumer           
| `npm run start:all`       | `concurrently ...`                   | Start API, consumer, and simulator together 
| `npm test`                | `jest`                               | Run all tests                      
| `npm run test:unit`       | `jest --testPathPatterns=unit`       | Run unit tests only (no DB needed) 
| `npm run test:integration`| `jest --testPathPatterns=integration`| Run integration tests (requires test DB) 
| `npm run test:coverage`   | `jest --coverage`                    | Run all tests with coverage report 

---

## Environment Variables

| Variable                | Description                               | Default |
|-------------------------|-------------------------------------------|---------|
| `PORT`                  | HTTP server port                          | `3000`  |
| `DATABASE_URL`          | PostgreSQL connection string              | —       |
| `TEST_DATABASE_URL`     | Separate DB for integration tests         | —       |
| `KAFKA_BROKER`          | Kafka broker address (`host:port`)        | —       |
| `SIMULATOR_INTERVAL_MS` | How often the simulator ticks per vehicle | `1000`  |
| `LOG_LEVEL`             | Pino log level (`info`, `debug`, etc.)    | `info`  |

---

## API Endpoints

All routes are mounted under `/telemetry`.

| Method |                  Path                      |                     Description                      
|--------|--------------------------------------------|------------------------------------------------------
| `POST` | `/telemetry`                               | Accept a telemetry event, validate, publish to Kafka 
| `GET`  | `/telemetry/vehicles/:id/latest`           | Most recent raw event for a vehicle 
| `GET`  | `/telemetry/vehicles/:id/history?limit=50` | Paginated raw event log (max 500) 
| `GET`  | `/telemetry/vehicles/:id/aggregates`       | Last 10 Flink 30s windows for a vehicle 
| `GET`  | `/telemetry/fleet/stats`                   | Fleet-wide summary: status counts + avg speed 

Example requests:

```bash
# Latest position for VH-001
curl http://localhost:3000/telemetry/vehicles/VH-001/latest

# Last 100 raw events for VH-003
curl "http://localhost:3000/telemetry/vehicles/VH-003/history?limit=100"

# Flink aggregation windows for VH-002
curl http://localhost:3000/telemetry/vehicles/VH-002/aggregates

# Fleet-wide summary
curl http://localhost:3000/telemetry/fleet/stats
```

---

## MCP Server (Claude Desktop integration)

The `mcp/` directory contains a standalone MCP server that exposes the pipeline to Claude as callable tools. Instead of writing curl commands, you ask Claude in plain English — "which vehicles are speeding right now?" — and it queries the database and answers.

Build it once:

```bash
cd mcp
npm install
npm run build
```

Then see [mcp/README.md](mcp/README.md) for the full setup: how to add the server to `claude_desktop_config.json`, example queries, and prerequisites.

---

## Running Tests

### Unit tests — no infrastructure needed

Kafka and the database are both mocked. Run anywhere, no Docker required, no app server needed.

```bash
npm run test:unit
```

### Integration tests — require Docker Postgres only

The integration tests hit a real database. You do **not** need the API server running — Supertest starts its own internal server automatically. You do **not** need Kafka or Flink.

One-time setup (creates and migrates the test database):

```bash
docker-compose up -d
psql postgres://fleet:fleet@localhost:5433/postgres -c "CREATE DATABASE fleet_pulse_test;"
DATABASE_URL=postgres://fleet:fleet@localhost:5433/fleet_pulse_test npm run migrate
```

Then run any time:

```bash
npm run test:integration
```

### Run everything

```bash
npm test              # all tests
npm run test:coverage # all tests with coverage report
```
