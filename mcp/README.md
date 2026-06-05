# Fleet Pulse MCP Server

The MCP (Model Context Protocol) server wraps the Fleet Pulse pipeline and exposes it to Claude as a set of callable tools. Instead of writing curl commands to query vehicle data, you ask Claude in plain English — it decides which tool to call, runs the query, and gives you a natural language answer.

## What is MCP?

MCP is an open protocol that lets AI assistants call external tools and data sources in a standardised way. Claude Desktop acts as the host; this server acts as a plugin that registers tools Claude can invoke. When you ask "which vehicles are speeding right now?", Claude calls `list_speeding_vehicles`, gets back JSON, and summarises it in plain English.

## Tools

|           Tool           |                    Example question 
|--------------------------|---------------------------------------------------------
| `get_vehicle_latest`     | "Where is VH-001 right now?" 
| `get_vehicle_aggregates` | "What has VH-002's average speed been over the last few windows?" 
| `list_speeding_vehicles` | "Which vehicles are doing over 100 km/h in the last 10 minutes?" 
| `get_idle_vehicles`      | "Which vehicles have been sitting idle for more than 15 minutes?" 
| `get_fleet_stats`        | "Give me a full fleet summary." 
| `get_kafka_lag`          | "Is the pipeline keeping up? How far is the consumer behind?" 

## Setup

### 1. Install and build

From the `mcp/` directory:

```bash
cd mcp
npm install
npm run build
```

### 2. Wire up Claude Desktop

Open your Claude Desktop config:

- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

Add the fleet-pulse server entry (replace the path with your actual project path):

```json
{
  "mcpServers": {
    "fleet-pulse": {
      "command": "node",
      "args": ["/absolute/path/to/fleet-pulse/mcp/dist/index.js"],
      "env": {
        "DATABASE_URL": "postgres://fleet:fleet@localhost:5433/fleet_pulse",
        "KAFKA_BROKER": "localhost:9092"
      }
    }
  }
}
```

### 3. Restart Claude Desktop

After saving the config, restart Claude Desktop. The fleet-pulse tools will appear in the tools panel.

### 4. Prerequisites

The MCP server reads from the same database and Kafka broker as the main pipeline. Make sure these are running before opening Claude Desktop:

```bash
docker-compose up -d    # from the project root
npm run start:all       # start API, simulator, and consumer
```

## Example queries

- "What is VH-001's current location and speed?"
- "Show me the last 5 aggregation windows for VH-003."
- "Are any vehicles exceeding 120 km/h right now?"
- "Which vehicles have been idle for more than 10 minutes?"
- "Give me a fleet-wide summary — how many vehicles are moving?"
- "Is the Kafka consumer keeping up with the simulator?"
