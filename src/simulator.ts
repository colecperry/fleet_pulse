// simulator.ts — pretends to be a fleet of 5 vehicles, sending telemetry events over HTTP every second.
//
// Flow: simulator.ts → POST /telemetry → Kafka → consumer.ts → PostgreSQL
//
// The simulator acts as vehicle firmware: it knows nothing about Kafka.
// It speaks HTTP to the ingestion gateway (POST /telemetry), which validates the payload and publishes to Kafka. This matches the production pattern — real telematics devices talk HTTP

import dotenv from 'dotenv';
dotenv.config();

const PORT = process.env.PORT || 3000;
const API_URL = `http://localhost:${PORT}/telemetry`;
const INTERVAL_MS = parseInt(process.env.SIMULATOR_INTERVAL_MS || '1000', 10);

// Each vehicle starts at a fixed lat/lon and drifts from there each tick.
// Starting coords are spread around a central point to simulate a real city fleet.
interface Vehicle {
  id: string;
  lat: number;
  lon: number;
  speed_kmph: number;
}

const vehicles: Vehicle[] = [ // array of 5 vehicles, which are then mutated in place
  { id: 'VH-001', lat: 37.7749, lon: -122.4194, speed_kmph: 0 },
  { id: 'VH-002', lat: 37.7751, lon: -122.4180, speed_kmph: 0 },
  { id: 'VH-003', lat: 37.7740, lon: -122.4210, speed_kmph: 0 },
  { id: 'VH-004', lat: 37.7760, lon: -122.4175, speed_kmph: 0 },
  { id: 'VH-005', lat: 37.7735, lon: -122.4220, speed_kmph: 0 },
];

// Helper fn to derive status from speed - return type must be one of the three string literals
function deriveStatus(speed: number): 'stopped' | 'idle' | 'moving' {
  if (speed < 2) return 'stopped';
  if (speed < 10) return 'idle';
  return 'moving';
}

// Move lat/long by a small random delta each tick to simulate realistic drift.
function nudgeCoord(value: number): number {
  return value + (Math.random() - 0.5) * 0.001;
}

// Adjust speed by a random delta each tick, clamped to [0, 140] kmph.
function nudgeSpeed(current: number): number {
  const delta = (Math.random() - 0.5) * 20;
  return Math.max(0, Math.min(140, current + delta));
}

// main simulator loop — runs every INTERVAL_MS, updates each vehicle's state, and sends a telemetry event to Kafka. Runs indefinitely until the user presses Ctrl+C, which triggers the SIGINT handler to clean up and exit.

async function run() {
  console.log(`Simulator starting. POSTing to ${API_URL} every ${INTERVAL_MS}ms...`);

    // setInterval takes a function to run, and how often to run it in ms. It runs this function on a loop until we call clearInterval.
  const tick = setInterval(async () => {
    for (const vehicle of vehicles) {
      vehicle.lat = nudgeCoord(vehicle.lat);
      vehicle.lon = nudgeCoord(vehicle.lon);
      vehicle.speed_kmph = nudgeSpeed(vehicle.speed_kmph);

      const status = deriveStatus(vehicle.speed_kmph);

      // payload matches the TelemetryEvent interface
      const payload = {
        vehicle_id: vehicle.id,
        lat: vehicle.lat,
        lon: vehicle.lon,
        speed_kmph: parseFloat(vehicle.speed_kmph.toFixed(2)),
        status,
        recorded_at: new Date().toISOString(),
      };
      // send POST request to the API server with the telemetry event as JSON in the body. 
      try {
        const res = await fetch(API_URL, { // Await the response so we can log success/failure
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });

        if (!res.ok) {
          console.error(`[${vehicle.id}] POST failed: ${res.status}`);
        } else {
          console.log(`[${vehicle.id}] speed=${payload.speed_kmph} kmph  status=${status}`);
        }
      } catch (err) {
        console.error(`[${vehicle.id}] Could not reach API — is npm run dev running?`, err);
      }
    }
  }, INTERVAL_MS);

  process.on('SIGINT', () => {
    console.log('\nShutting down simulator...');
    clearInterval(tick);
    process.exit(0);
  });
}

if (require.main === module) {
  run().catch((err) => {
    console.error('Simulator error:', err);
    process.exit(1);
  });
}
