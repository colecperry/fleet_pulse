// simulator.ts — pretends to be a fleet of 5 vehicles and sends telemetry events to Kafka every second.
//
// Flow: simulator.ts → Kafka → consumer.ts → PostgreSQL

import dotenv from 'dotenv';
dotenv.config();

import { sendTelemetryEvent, disconnectProducer, TelemetryEvent } from './producer';

const INTERVAL_MS = parseInt(process.env.SIMULATOR_INTERVAL_MS || '1000', 10);

// Each vehicle starts at a fixed lat/lon and drifts from there each tick.
// Starting coords are spread around a central point to simulate a real city fleet.
interface Vehicle { // define the shape of our in-memory vehicle objects
  id: string;
  lat: number;
  lon: number;
  speed_kmph: number;
}

const vehicles: Vehicle[] = [ // array of 5 vehicles, which is then mutated
  { id: 'VH-001', lat: 37.7749, lon: -122.4194, speed_kmph: 0 },
  { id: 'VH-002', lat: 37.7751, lon: -122.4180, speed_kmph: 0 },
  { id: 'VH-003', lat: 37.7740, lon: -122.4210, speed_kmph: 0 },
  { id: 'VH-004', lat: 37.7760, lon: -122.4175, speed_kmph: 0 },
  { id: 'VH-005', lat: 37.7735, lon: -122.4220, speed_kmph: 0 },
];

// Helper function to derive status from speed - return type must be one of the three string literals
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
  console.log('Simulator starting. Publishing telemetry...');

  // setInterval takes a function to run, and how often to run it in ms. It runs this function on a loop until we call clearInterval.
  const tick = setInterval(async () => {
    for (const vehicle of vehicles) {
      // Mutate state in place — each tick builds on the previous position and speed.
      vehicle.lat = nudgeCoord(vehicle.lat);
      vehicle.lon = nudgeCoord(vehicle.lon);
      vehicle.speed_kmph = nudgeSpeed(vehicle.speed_kmph);

      const status = deriveStatus(vehicle.speed_kmph);

      const event: TelemetryEvent = { // create actual telemetry event object to send to Kafka
        vehicle_id: vehicle.id,
        lat: vehicle.lat,
        lon: vehicle.lon,
        speed_kmph: parseFloat(vehicle.speed_kmph.toFixed(2)),
        status,
        recorded_at: new Date().toISOString(),
      };

      // Delegate publishing to the shared producer module —
      await sendTelemetryEvent(event);

      console.log(`[${vehicle.id}] speed=${event.speed_kmph} kmph  status=${status}`);
    }
  }, INTERVAL_MS);

  // SIGINT = Ctrl+C -> event listener for when the user presses Ctrl+C to stop the simulator. This is the only way to stop the simulator.
  process.on('SIGINT', async () => {
    console.log('\nShutting down simulator...');
    clearInterval(tick); // stops the repeating tick function, so we stop publishing new events
    await disconnectProducer(); // disconnect the producer from Kafka -> clean shutdown
    process.exit(0);
  });
}

// Run the simulator script
if (require.main === module) {
  run().catch((err) => {
    console.error('Simulator error:', err);
    process.exit(1);
  });
}
