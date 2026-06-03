// routes/telemetry.ts — ingestion and read endpoints for the fleet telemetry pipeline.
//
// POST /telemetry           — accepts events from the simulator, validates, publishes to Kafka
// GET  /vehicles/:id/latest     — most recent raw event for a vehicle
// GET  /vehicles/:id/history    — paginated raw event log for a vehicle
// GET  /vehicles/:id/aggregates — pre-computed 30s Flink windows for a vehicle
// GET  /fleet/stats             — fleet-wide summary from the latest aggregation window
//
// Why separate endpoints rather than one generic query endpoint?
// Each endpoint has a fixed, known query shape — the DB index is designed for it and the response contract is stable. A generic endpoint (e.g. ?field=&op=&value=) would require dynamic SQL, making injection prevention harder and query planning unpredictable.
//
// Why serve aggregates from vehicle_aggregates rather than computing on the fly?
// Computing AVG/COUNT over raw telemetry_events at query time means scanning potentially millions of rows on every request. Flink pre-computes those windows every 30 seconds and writes one row per vehicle — the API just reads that row. Reads stay fast regardless of how much raw data accumulates.

import { Router, Request, Response } from 'express';
import { sendTelemetryEvent, TelemetryEvent } from '../producer';
import pool from '../db';

// The express router takes two arguments:
// 1. The path (e.g. '/telemetry') — this is defined in src/index.ts 
// 2. A callback function that runs when a request hits this endpoint, with the request and response objects as arguments. We must mark this function as async so we can use await inside.
const router = Router();

const VALID_STATUSES = ['moving', 'idle', 'stopped'] as const;

/**
 * POST /telemetry
 * Accepts a telemetry event, validates it, and publishes to Kafka.
 * Returns 202 because the event is queued in Kafka — not yet written to PostgreSQL.
 */
router.post('/', async (req: Request, res: Response) => {
  // destructure the expected fields from the request body.
  const { vehicle_id, lat, lon, speed_kmph, status } = req.body;

  if ( // validation — check required fields are present and of the correct type
    typeof vehicle_id !== 'string' ||
    typeof lat !== 'number' ||
    typeof lon !== 'number' ||
    typeof speed_kmph !== 'number' ||
    !VALID_STATUSES.includes(status)
  ) {
    res.status(400).json({ // Bad Request — the client sent something we didn't understand
      error: 'Invalid payload',
      required: {
        vehicle_id: 'string',
        lat: 'number',
        lon: 'number',
        speed_kmph: 'number',
        status: 'moving | idle | stopped',
      },
    });
    return;
  }

  const event: TelemetryEvent = { // Create a TelemetryEvent object to send to Kafka. 
    vehicle_id,
    lat,
    lon,
    speed_kmph,
    status,
    recorded_at: new Date().toISOString(),
  };

  await sendTelemetryEvent(event); // Send the event to Kafka. Await to ensure it lands in Kafka before we respond to the client.
  res.status(202).json({ message: 'Event accepted' }); // Send 202 response to caller 
});

/**
 * GET /vehicles/:id/latest
 * Returns the single most recent telemetry event for a vehicle.
 * Uses the composite index (vehicle_id, recorded_at DESC) — no full table scan.
 */
router.get('/vehicles/:id/latest', async (req: Request, res: Response) => {
  const { id } = req.params; // desctructure the vehicle ID from the request URL path

  // Query the telemetry_events table for the most recent event for this vehicle. The index on (vehicle_id, recorded_at DESC) allows Postgres to efficiently jump to the relevant rows and return the latest one without scanning the whole table. 
  // $1 placeholder is replaced by the id variable to prevent SQL injection
  const result = await pool.query( // this returns a result object, with a .rows property
    `SELECT * FROM telemetry_events
     WHERE vehicle_id = $1 
     ORDER BY recorded_at DESC
     LIMIT 1`,
    [id]
  );

  if (result.rows.length === 0) {
    res.status(404).json({ error: `No events found for vehicle ${id}` });
    return;
  }

  // res.json is how you send data back to the client in Express (no return needed)
  res.json(result.rows[0]); // return only the single most recent event (first row in result) 
});

// GET /vehicles/:id/history?limit=50
// Returns a paginated list of raw events for a vehicle, newest first.
// limit defaults to 50, capped at 500 — uncapped history on a busy fleet would return
// millions of rows and make the response unusably large.
// limit is interpolated directly (not parameterized) because it's already validated as
// a safe integer — pg's LIMIT $n can behave inconsistently with numeric params.
router.get('/vehicles/:id/history', async (req: Request, res: Response) => {
  const { id } = req.params;
  const rawLimit = parseInt((req.query.limit as string) || '50', 10);
  const limit = Math.min(isNaN(rawLimit) || rawLimit < 1 ? 50 : rawLimit, 500);

  const result = await pool.query(
    `SELECT * FROM telemetry_events
     WHERE vehicle_id = $1
     ORDER BY recorded_at DESC
     LIMIT ${limit}`,
    [id]
  );

  res.json(result.rows);
});

/**
 * GET /vehicles/:id/aggregates
 * Returns the 10 most recent 30-second Flink aggregation windows for a vehicle.
 * Each window contains avg_speed_kmph, event_count, window_start, and window_end.
 */
router.get('/vehicles/:id/aggregates', async (req: Request, res: Response) => {
  const { id } = req.params;

  const result = await pool.query(
    `SELECT avg_speed_kmph, event_count, dominant_status, window_start, window_end
     FROM vehicle_aggregates
     WHERE vehicle_id = $1
     ORDER BY window_start DESC
     LIMIT 10`,
    [id]
  );

  res.json(result.rows);
});

// GET /fleet/stats
// Status counts come from telemetry_events (latest event per vehicle) because
// dominant_status in vehicle_aggregates is NULL — Flink SQL has no MODE() function.
// avg_fleet_speed comes from vehicle_aggregates where Flink pre-computes it correctly.
router.get('/fleet/stats', async (_req: Request, res: Response) => {
  const [statusResult, speedResult] = await Promise.all([
    // DISTINCT ON picks the single most recent event per vehicle in one index scan
    pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE status = 'moving')  AS moving,
        COUNT(*) FILTER (WHERE status = 'idle')    AS idle,
        COUNT(*) FILTER (WHERE status = 'stopped') AS stopped
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

  const s = statusResult.rows[0];
  const sp = speedResult.rows[0];
  res.json({
    moving: parseInt(s.moving, 10),
    idle: parseInt(s.idle, 10),
    stopped: parseInt(s.stopped, 10),
    avg_fleet_speed: parseFloat(sp.avg_fleet_speed) || 0,
  });
});

export default router;
