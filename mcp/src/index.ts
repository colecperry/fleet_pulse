// mcp/src/index.ts — Fleet Pulse MCP Server
//
// This process exposes pipeline data to Claude as callable tools. Claude decides which tool to invoke based on the user's natural language question. The server communicates over stdio — Claude Desktop spawns it as a child process and sends/receives JSON-RPC messages on stdin/stdout.
//
// Flow: Claude Desktop → spawns this process → tool call → PostgreSQL / Kafka → JSON → Claude

import path from 'path';
import dotenv from 'dotenv';
// Use __dirname so the .env path is always resolved relative to this file,
// not relative to whatever directory Claude Desktop happens to launch from.
dotenv.config({ path: path.resolve(__dirname, '../../.env') });

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';
import { Pool } from 'pg';
import { Kafka } from 'kafkajs';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const kafka = new Kafka({
  clientId: 'fleet-mcp',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

const server = new McpServer({
  name: 'fleet-pulse',
  version: '1.0.0',
});

// ─── Tool 1: get_vehicle_latest ───────────────────────────────────────────────
// Returns the single most recent telemetry event for a vehicle.
// Example: "Where is VH-001 right now?" "What speed is VH-003 doing?"
// @ts-ignore — TS2589: MCP SDK generics exceed TypeScript's type instantiation depth limit; code is correct
server.registerTool(
  'get_vehicle_latest', // tool name in Claude
  {
    // description is what Claude actually reads to understand when to call this tool, inputSchema defines the expected input shape Claude must send, Zod validates it at runtime
    description: 'Get the most recent telemetry event for a specific vehicle',
    inputSchema: { vehicle_id: z.string() },
  },
  async ({ vehicle_id }) => { // callback function that runs when tool is called
    const result = await pool.query(
      `SELECT vehicle_id, lat, lon, speed_kmph, status, recorded_at
       FROM telemetry_events
       WHERE vehicle_id = $1
       ORDER BY recorded_at DESC
       LIMIT 1`,
      [vehicle_id]
    );
    if (result.rows.length === 0) {
      return { content: [{ type: 'text' as const, text: `No events found for vehicle ${vehicle_id}` }] };
    }
    // the MCP SDK expects tool responses in this format -> an array in case multiple things are returned, then convert the JS object into formatted JSON so Claude can read the response
    return { content: [{ type: 'text' as const, text: JSON.stringify(result.rows[0], null, 2) }] };
  }
);

// ─── Tool 2: get_vehicle_aggregates ──────────────────────────────────────────
// Averages Flink 30-second windows over a human-readable time range.
// Claude asks "what's VH-001's average speed over the last 5 minutes?" and gets a single computed answer — not raw rows it has to average itself.
// Example: "What's VH-002's average speed lately?" "How busy has VH-001 been?"
server.registerTool(
  'get_vehicle_aggregates',
  {
    description: 'Get the average speed and average event count for a vehicle over the last N minutes, computed across Flink 30-second windows',
    inputSchema: {
      vehicle_id: z.string(),
      since_minutes: z.number().optional().default(5),
    },
  },
  async ({ vehicle_id, since_minutes }) => {
    // use since_minutes if provided, if not, fallback to 5 mins, cap at 60 mins
    const minutes = Math.min(since_minutes ?? 5, 60);
    // Pull all 30 second windows for this vehicle within time range and collapses into one row
    // How to READ -> FROM, WHERE/AND, SELECT -> Aggregate functions return one row
    const result = await pool.query(
      `SELECT
         ROUND(AVG(avg_speed_kmph)::numeric, 2) AS avg_speed_kmph,
         ROUND(AVG(event_count)::numeric, 2)    AS avg_event_count,
         SUM(event_count)                        AS total_events,
         COUNT(*)                                AS windows_covered,
         MIN(window_start)                       AS period_start,
         MAX(window_end)                         AS period_end
       FROM vehicle_aggregates
       WHERE vehicle_id = $1
         AND window_start > NOW() - INTERVAL '${minutes} minutes'`,
      [vehicle_id]
    );
    if (!result.rows[0].avg_speed_kmph) {
      return { content: [{ type: 'text' as const, text: `No aggregate data found for vehicle ${vehicle_id} in the last ${minutes} minutes.` }] };
    }
    return { content: [{ type: 'text' as const, text: JSON.stringify({ vehicle_id, since_minutes: minutes, ...result.rows[0] }, null, 2) }] };
  }
);

// ─── Tool 3: list_speeding_vehicles ──────────────────────────────────────────
// Finds vehicles exceeding a speed threshold in a recent time window.
// Example: "Which vehicles are doing over 100 kmph in the last 10 minutes?"
server.registerTool(
  'list_speeding_vehicles',
  {
    description: 'Find vehicles exceeding a speed threshold in the last N minutes',
    inputSchema: {
      threshold_kmph: z.number().optional().default(120),
      since_minutes: z.number().optional().default(5),
    },
  },
  async ({ threshold_kmph, since_minutes }) => {
    const minutes = since_minutes ?? 5;
    const threshold = threshold_kmph ?? 120;
    // FROM -> WHERE/AND -> ORDER BY -> DISINCT ON
    const result = await pool.query(
      `SELECT DISTINCT ON (vehicle_id) vehicle_id, speed_kmph, recorded_at
       FROM telemetry_events
       WHERE speed_kmph > $1
         AND recorded_at > NOW() - INTERVAL '${minutes} minutes'
       ORDER BY vehicle_id, recorded_at DESC`,
      [threshold]
    );
    if (result.rows.length === 0) {
      return {
        content: [{
          type: 'text' as const,
          text: `No vehicles exceeding ${threshold} km/h in the last ${minutes} minutes.`,
        }],
      };
    }
    return { content: [{ type: 'text' as const, text: JSON.stringify(result.rows, null, 2) }] };
  }
);

// ─── Tool 4: get_idle_vehicles ────────────────────────────────────────────────
// Finds vehicles whose most recent event is 'idle' and has been so for at least N minutes.
// Example: "Which vehicles have been sitting idle for over 15 minutes?"
server.registerTool(
  'get_idle_vehicles',
  {
    description: 'Find vehicles that have been idle for at least N minutes',
    inputSchema: {
      min_idle_minutes: z.number().optional().default(10),
    },
  },
  async ({ min_idle_minutes }) => {
    const minutes = min_idle_minutes ?? 10;
    // READ INNER QUERY - FROM -> ORDER BY -> SELECT DISTINCT ON
    // Go to telemetry events, sort by vehicle then newest first, keep only the most recent event per vehicle
    // READ OUTER QUERY - FROM (...) latest -> WHERE -> AND -> SELECT -> ROUND -> ORDER BY
    // Query result of inner table as variable name latest, filter to vehicles whose most recent event was 'idle', filter further by N minutes ago, grab the vehicle ID and when it went idle, calculate how many minutes ago that idle event was, then sort by most idle first
    const result = await pool.query(
      `SELECT vehicle_id, recorded_at,
         ROUND(EXTRACT(EPOCH FROM (NOW() - recorded_at)) / 60)::integer AS idle_minutes
       FROM (
         SELECT DISTINCT ON (vehicle_id) vehicle_id, recorded_at, status
         FROM telemetry_events
         ORDER BY vehicle_id, recorded_at DESC
       ) latest
       WHERE status = 'idle'
         AND recorded_at < NOW() - INTERVAL '${minutes} minutes'
       ORDER BY idle_minutes DESC`
    );
    if (result.rows.length === 0) {
      return {
        content: [{
          type: 'text' as const,
          text: `No vehicles have been idle for more than ${minutes} minutes.`,
        }],
      };
    }
    return { content: [{ type: 'text' as const, text: JSON.stringify(result.rows, null, 2) }] };
  }
);

// ─── Tool 5: get_fleet_stats ──────────────────────────────────────────────────
// Fleet-wide snapshot: status counts + average speed across all vehicles at current time
// Status comes from telemetry_events (real-time), avg speed from vehicle_aggregates (Flink).
// Example: "Give me a fleet summary." "How many vehicles are moving right now?"
server.registerTool(
  'get_fleet_stats',
  {
    description: 'Get a real-time fleet-wide summary: status counts and average speed',
  },
  async () => { // no params, 2 queries running in parallel, promise.all() fires both
    const [statusResult, speedResult] = await Promise.all([
      pool.query(`
        SELECT
          COUNT(*) FILTER (WHERE status = 'moving')  AS moving,
          COUNT(*) FILTER (WHERE status = 'idle')    AS idle,
          COUNT(*) FILTER (WHERE status = 'stopped') AS stopped,
          COUNT(*) AS active_vehicles
        FROM (
          SELECT DISTINCT ON (vehicle_id) vehicle_id, status
          FROM telemetry_events
          ORDER BY vehicle_id, recorded_at DESC
        ) latest
      `),
      pool.query(`
        SELECT ROUND(AVG(avg_speed_kmph)::numeric, 2) AS avg_fleet_speed
        FROM (
          SELECT DISTINCT ON (vehicle_id) vehicle_id, avg_speed_kmph
          FROM vehicle_aggregates
          ORDER BY vehicle_id, window_start DESC
        ) latest
      `),
    ]);
    const s = statusResult.rows[0]; // status results
    const sp = speedResult.rows[0]; // speed results
    const stats = {
      moving: parseInt(s.moving, 10),
      idle: parseInt(s.idle, 10),
      stopped: parseInt(s.stopped, 10),
      active_vehicles: parseInt(s.active_vehicles, 10),
      avg_fleet_speed_kmph: parseFloat(sp.avg_fleet_speed) || 0,
    };
    return { content: [{ type: 'text' as const, text: JSON.stringify(stats, null, 2) }] };
  }
);

// ─── Tool 6: get_kafka_lag ────────────────────────────────────────────────────
// Lag = how many messages the consumer group is behind the latest offset.
// Our consumer is ingesting telemetry events from Kafka, and writing them to Postgres. We need to make sure that our consumer is not falling behind.
server.registerTool(
  'get_kafka_lag',
  {
    description: 'Check the consumer group lag — how far behind is the telemetry consumer?',
  },
  async () => { // no params
    const admin = kafka.admin(); // Create a Kafka admin client and connects it 
    await admin.connect(); // the admin is used to inspect the cluster
    try {
      const [topicOffsets, groupOffsets] = await Promise.all([
        // fetch Kafka's latest message and where the consumer has read up to
        admin.fetchTopicOffsets('telemetry'), 
        admin.fetchOffsets({ groupId: 'telemetry-service', topics: ['telemetry'] }), 
      ]);

      // pull out consumer's partition data and calculate total lag
      const committed = groupOffsets[0]?.partitions ?? [];
      let total_lag = 0;
      for (const topic of topicOffsets) {
        const consumer = committed.find(p => p.partition === topic.partition)?.offset ?? '0';
        total_lag += Math.max(0, parseInt(topic.offset, 10) - parseInt(consumer, 10));
      }

      return {
        content: [{
          type: 'text' as const,
          text: JSON.stringify({ total_lag, healthy: total_lag === 0 }, null, 2),
        }],
      };
    } finally {
      await admin.disconnect();
    }
  }
);

// Connect the server to stdio transport.
// Claude Desktop spawns this process and sends JSON-RPC over stdin/stdout.
async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch((err) => {
  process.stderr.write(`MCP server error: ${err}\n`);
  process.exit(1);
});
