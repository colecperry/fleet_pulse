// simulator.ts — pretends to be a fleet of 5 vehicles, sending telemetry events over HTTP every second.
//
// Flow: simulator.ts → POST /telemetry → Kafka → consumer.ts → PostgreSQL
//
// The simulator acts as vehicle firmware: it knows nothing about Kafka.
// It speaks HTTP to the ingestion gateway (POST /telemetry), which validates the payload and publishes to Kafka.

import dotenv from 'dotenv';
dotenv.config();

const PORT = process.env.PORT || 3000;
const API_URL = `http://localhost:${PORT}/telemetry`;
const INTERVAL_MS = parseInt(process.env.SIMULATOR_INTERVAL_MS || '1000', 10);

// How long a vehicle stays idle before resuming movement (in ticks).
const IDLE_DURATION_TICKS = 90; // ~90 seconds at 1s interval
// Probability each moving vehicle enters idle on any given tick.
const IDLE_ENTRY_PROBABILITY = 0.002; // ~0.2% per tick → roughly once every 8 minutes per vehicle

type VehicleState = 'moving' | 'idle';

interface Vehicle {
  id: string;
  lat: number;
  lon: number;
  speed_kmph: number;
  state: VehicleState;   // current behaviour mode
  idleTicksLeft: number; // countdown — how many ticks remain in the idle state
}

const vehicles: Vehicle[] = [
  { id: 'VH-001', lat: 37.7749, lon: -122.4194, speed_kmph: 0, state: 'moving', idleTicksLeft: 0 },
  { id: 'VH-002', lat: 37.7751, lon: -122.4180, speed_kmph: 0, state: 'moving', idleTicksLeft: 0 },
  { id: 'VH-003', lat: 37.7740, lon: -122.4210, speed_kmph: 0, state: 'moving', idleTicksLeft: 0 },
  { id: 'VH-004', lat: 37.7760, lon: -122.4175, speed_kmph: 0, state: 'moving', idleTicksLeft: 0 },
  { id: 'VH-005', lat: 37.7735, lon: -122.4220, speed_kmph: 0, state: 'moving', idleTicksLeft: 0 },
];

function nudgeCoord(value: number): number {
  return value + (Math.random() - 0.5) * 0.001;
}

function nudgeSpeed(current: number): number {
  const delta = (Math.random() - 0.5) * 20;
  return Math.max(0, Math.min(140, current + delta));
}

// Advance one vehicle's state machine by one tick.
// Returns the speed and status to emit for this tick.
function tick(vehicle: Vehicle): { speed_kmph: number; status: 'moving' | 'idle' | 'stopped' } {
  if (vehicle.state === 'idle') {
    // Stay idle — emit near-zero speed so the MCP idle query matches.
    vehicle.idleTicksLeft -= 1;
    if (vehicle.idleTicksLeft <= 0) {
      vehicle.state = 'moving'; // resume movement after idle period
    }
    const idleSpeed = parseFloat((Math.random() * 1.5).toFixed(2)); // 0–1.5 km/h
    vehicle.speed_kmph = idleSpeed;
    return { speed_kmph: idleSpeed, status: 'idle' };
  }

  // Moving — small random chance to transition into idle
  if (Math.random() < IDLE_ENTRY_PROBABILITY) {
    vehicle.state = 'idle';
    vehicle.idleTicksLeft = IDLE_DURATION_TICKS;
    vehicle.speed_kmph = 0;
    return { speed_kmph: 0, status: 'idle' };
  }

  // Normal moving tick
  vehicle.speed_kmph = nudgeSpeed(vehicle.speed_kmph);
  const status = vehicle.speed_kmph < 2 ? 'stopped' : 'moving';
  return { speed_kmph: parseFloat(vehicle.speed_kmph.toFixed(2)), status };
}

async function run() {
  console.log(`Simulator starting. POSTing to ${API_URL} every ${INTERVAL_MS}ms...`);

  const interval = setInterval(async () => {
    for (const vehicle of vehicles) {
      vehicle.lat = nudgeCoord(vehicle.lat);
      vehicle.lon = nudgeCoord(vehicle.lon);

      const { speed_kmph, status } = tick(vehicle);

      const payload = {
        vehicle_id: vehicle.id,
        lat: vehicle.lat,
        lon: vehicle.lon,
        speed_kmph,
        status,
        recorded_at: new Date().toISOString(),
      };

      try {
        const res = await fetch(API_URL, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });

        if (!res.ok) {
          console.error(`[${vehicle.id}] POST failed: ${res.status}`);
        } else {
          console.log(`[${vehicle.id}] speed=${speed_kmph} kmph  status=${status}`);
        }
      } catch (err) {
        console.error(`[${vehicle.id}] Could not reach API — is npm run dev running?`, err);
      }
    }
  }, INTERVAL_MS);

  process.on('SIGINT', () => {
    console.log('\nShutting down simulator...');
    clearInterval(interval);
    process.exit(0);
  });
}

if (require.main === module) {
  run().catch((err) => {
    console.error('Simulator error:', err);
    process.exit(1);
  });
}
