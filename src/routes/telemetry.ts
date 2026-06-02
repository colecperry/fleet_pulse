import { Router, Request, Response } from 'express';
import { sendTelemetryEvent, TelemetryEvent } from '../producer';

const router = Router(); // create a new router instance (mini epress app that only handles routes defined in this file) — we export this and use it in src/index.ts to add these routes to the main Express app

// Allowed status values — defined as a constant so the validation and the type stay in sync.
const VALID_STATUSES = ['moving', 'idle', 'stopped'] as const;

// POST /telemetry — accepts a telemetry event from any client (vehicle ECU, simulator, curl)
// and publishes it to the Kafka 'telemetry' topic.
//
// Returns 202 Accepted rather than 201 Created because the event has been received and queued in Kafka, but it hasn't been persisted to PostgreSQL yet
router.post('/', async (req: Request, res: Response) => {
  // destructure the expected fields from the JSON body of the request
  const { vehicle_id, lat, lon, speed_kmph, status } = req.body; 

  // Validate presence and types of all required fields.
  if (
    typeof vehicle_id !== 'string' ||
    typeof lat !== 'number' ||
    typeof lon !== 'number' ||
    typeof speed_kmph !== 'number' ||
    !VALID_STATUSES.includes(status)
  ) {
    // If validation fails, return a 400 Bad Request with an error message and the expected schema for reference.
    res.status(400).json({
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

  const event: TelemetryEvent = {
    vehicle_id,
    lat,
    lon,
    speed_kmph,
    status,
    recorded_at: new Date().toISOString(),
  };

  await sendTelemetryEvent(event); // publish event to Kafka - await before repsonding

  res.status(202).json({ message: 'Event accepted' });
});

export default router;
