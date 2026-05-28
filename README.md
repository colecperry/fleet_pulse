# Fleet Pulse

A real-time fleet telemetry pipeline using TypeScript, Node.js, Kafka, Flink, PostgreSQL, and Docker Compose — extended with a Model Context Protocol (MCP) server that exposes live vehicle data and pipeline health to Claude as a set of queryable tools.

---

## Architecture

```
Simulator → Kafka → Flink → vehicle_aggregates
                  ↓
             Consumer → telemetry_events → REST API → MCP Server → Claude
```

---

## Prerequisites

- Node.js 20+
- Docker and Docker Compose
- Python 3.9+ (for the Flink job)
- Claude Desktop (for MCP integration)

---

## Setup

```bash
# 1. Clone the repo
git clone https://github.com/your-username/fleet-pulse.git
cd fleet-pulse

# 2. Copy the environment template and fill in your values
cp .env.example .env

# 3. Install Node dependencies
npm install
```

---

## Running Locally

```bash
# Start all infrastructure (Kafka, Zookeeper, PostgreSQL, Flink)
docker-compose up -d

# Start the API server with live reload
npm run dev
```

Verify the server is running:

```bash
curl http://localhost:3000/health
# { "status": "ok" }
```

Flink dashboard: [http://localhost:8081](http://localhost:8081)

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
│   └── index.ts          # Express entry point
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
| `npm run dev`   | `nodemon --exec ts-node src/index.ts` | Start server with live reload |
| `npm run build` | `tsc`                                 | Compile TypeScript to `dist/` |
| `npm start`     | `node dist/index.js`                  | Run compiled production build |

---

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `PORT` | HTTP server port | `3000` |
| `DATABASE_URL` | PostgreSQL connection string | — |
| `TEST_DATABASE_URL` | Separate DB for integration tests | — |
| `KAFKA_BROKER` | Kafka broker address (`host:port`) | — |
| `SIMULATOR_INTERVAL_MS` | How often the simulator ticks per vehicle | `1000` |
| `JWT_SECRET` | Secret used to sign and verify JWTs | — |
| `LOG_LEVEL` | Pino log level (`info`, `debug`, etc.) | `info` |

---

## Steps Completed

- [x] Step 1 — Project scaffold (TypeScript, Express, dotenv, nodemon)
- [x] Step 2 — Docker Compose infrastructure (Kafka, Zookeeper, PostgreSQL, Flink)
- [ ] Step 3 — PostgreSQL schema and connection pool
- [ ] Step 4 — Vehicle simulator
- [ ] Step 5 — Kafka producer and POST /telemetry route
- [ ] Step 6 — Flink stream processing job
- [ ] Step 7 — Kafka consumer
- [ ] Step 8 — REST API endpoints
- [ ] Step 9 — User auth (bcrypt + JWT)
- [ ] Step 10 — Structured logging (Pino)
- [ ] Step 11 — Tests (Jest + Supertest)
- [ ] Step 12 — CI/CD (GitHub Actions + Docker)
- [ ] Step 13 — MCP server
- [ ] Step 14 — Full README
