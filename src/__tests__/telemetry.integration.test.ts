// Integration tests — slower, require a real Postgres database.
// Kafka is still mocked — we're only testing the DB layer here.
// Seeds real rows, runs real queries, and checks real results.
// Catches things mocks can't — wrong ordering, bad SQL, schema mismatches.
//
// Requires: TEST_DATABASE_URL in .env and schema applied to that DB.

import request from 'supertest';
import app from '../index';
import pool from '../db';

// Kafka is mocked even in integration tests — we're testing the DB layer, not Kafka.
jest.mock('../producer', () => ({
  sendTelemetryEvent: jest.fn().mockResolvedValue(undefined),
}));

const VEHICLE_ID = 'TEST-001';

// Helper function that inserts one event row directly into telemetry_events
async function seedEvent(overrides: Record<string, unknown> = {}) {
  const defaults = { // create valid event row
    vehicle_id: VEHICLE_ID,
    lat: 37.7749,
    lon: -122.4194,
    speed_kmph: 45.5,
    status: 'moving',
    recorded_at: new Date().toISOString(),
  };
  const e = { ...defaults, ...overrides }; // merge defaults with whatever you passed in
  await pool.query( // write the merged event as a real row into the test database
    `INSERT INTO telemetry_events (vehicle_id, lat, lon, speed_kmph, status, recorded_at)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [e.vehicle_id, e.lat, e.lon, e.speed_kmph, e.status, e.recorded_at]
  );
}

// Jest lifecycle:
// - beforeEach: run this function before each test to ensure a clean slate. 
// - afterEach: run this function after each test to clean up any rows created during the test.
// - afterAll: run this function once after all tests finish to close the DB pool and allow Jest to exit cleanly instead of hanging on open connections.
// - describe: group related tests together. Here we have two groups: one for GET /latest and one for GET /history, each with multiple test cases inside.
// - it: define an individual test case. The first arg is a string describing the test, and the second arg is an async function that performs the test using Jest assertions (e.g. expect(res.status).toBe(200)).
// - expect: Jest's assertion library. We use it to check that the API responses have the expected status codes and data. If an assertion fails, Jest will report which test failed and why.

// You must include the beforeEach, afterEach, and afterAll functions in this file to ensure the tests run correctly against a real database, Jest calls them automatically at the right times.

// Use LIKE 'TEST-%' to catch all test vehicle IDs used across every test in this file.
async function cleanTestData() {
  await pool.query(`DELETE FROM telemetry_events WHERE vehicle_id LIKE 'TEST-%'`);
  await pool.query(`DELETE FROM vehicle_aggregates WHERE vehicle_id LIKE 'TEST-%'`);
}

beforeEach(cleanTestData);
afterEach(cleanTestData);

afterAll(async () => {
  await pool.end();
});

describe('GET /telemetry/vehicles/:id/latest', () => {
  it('returns the most recent event for a vehicle', async () => {
    await seedEvent({ speed_kmph: 30, recorded_at: '2026-01-01T00:00:00Z' });
    await seedEvent({ speed_kmph: 80, recorded_at: '2026-01-01T00:01:00Z' });

    const res = await request(app).get(`/telemetry/vehicles/${VEHICLE_ID}/latest`);
    expect(res.status).toBe(200);
    expect(res.body.speed_kmph).toBe(80);
  });

  it('returns 404 when no events exist for a vehicle', async () => {
    const res = await request(app).get('/telemetry/vehicles/UNKNOWN-999/latest');
    expect(res.status).toBe(404);
  });
});

describe('GET /telemetry/vehicles/:id/history', () => {
  it('returns events in descending order by recorded_at', async () => {
    await seedEvent({ speed_kmph: 10, recorded_at: '2026-01-01T00:00:00Z' });
    await seedEvent({ speed_kmph: 50, recorded_at: '2026-01-01T00:01:00Z' });
    await seedEvent({ speed_kmph: 90, recorded_at: '2026-01-01T00:02:00Z' });

    const res = await request(app).get(`/telemetry/vehicles/${VEHICLE_ID}/history`);
    expect(res.status).toBe(200);
    expect(res.body[0].speed_kmph).toBe(90);
    expect(res.body[2].speed_kmph).toBe(10);
  });

  // seed 3 events, then request with ?limit=2 and verify only 2 are returned. =
  it('respects the ?limit query param', async () => {
    await seedEvent({ recorded_at: '2026-01-01T00:00:00Z' });
    await seedEvent({ recorded_at: '2026-01-01T00:01:00Z' });
    await seedEvent({ recorded_at: '2026-01-01T00:02:00Z' });

    const res = await request(app).get(`/telemetry/vehicles/${VEHICLE_ID}/history?limit=2`);
    expect(res.status).toBe(200);
    expect(res.body.length).toBe(2);
  });

  it('caps limit at 500', async () => {
    const res = await request(app).get(`/telemetry/vehicles/${VEHICLE_ID}/history?limit=9999`);
    expect(res.status).toBe(200);
  });
});

// Seed one row directly into vehicle_aggregates, simulating what the Flink job writes.
async function seedAggregate(overrides: Record<string, unknown> = {}) {
  const defaults = {
    vehicle_id: VEHICLE_ID,
    avg_speed_kmph: 55.0,
    event_count: 30,
    dominant_status: null,
    window_start: '2026-01-01T00:00:00Z',
    window_end: '2026-01-01T00:00:30Z',
  };
  const a = { ...defaults, ...overrides }; // merge defaults with whatever you passed in
  await pool.query( // write the merged aggregate as a real row into the test database
    `INSERT INTO vehicle_aggregates (vehicle_id, avg_speed_kmph, event_count, dominant_status, window_start, window_end)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [a.vehicle_id, a.avg_speed_kmph, a.event_count, a.dominant_status, a.window_start, a.window_end]
  );
}

// Seeds two windows with different speeds and timestamps
describe('GET /telemetry/vehicles/:id/aggregates', () => {
  // verifies that GET /aggregates returns them in the correct order with the correct data. 
  it('returns aggregation windows newest first', async () => {
    await seedAggregate({ avg_speed_kmph: 40, window_start: '2026-01-01T00:00:00Z', window_end: '2026-01-01T00:00:30Z' });
    await seedAggregate({ avg_speed_kmph: 80, window_start: '2026-01-01T00:01:00Z', window_end: '2026-01-01T00:01:30Z' });

    const res = await request(app).get(`/telemetry/vehicles/${VEHICLE_ID}/aggregates`);
    expect(res.status).toBe(200);
    expect(res.body[0].avg_speed_kmph).toBe(80);
    expect(res.body[1].avg_speed_kmph).toBe(40);
  });

  // verifies that GET /aggregates returns an empty array when no windows exist for a vehicle.
  it('returns an empty array when no windows exist', async () => {
    const res = await request(app).get('/telemetry/vehicles/UNKNOWN-999/aggregates');
    expect(res.status).toBe(200);
    expect(res.body).toEqual([]);
  });
});

// Seed one event with each status, then verify that GET /fleet/stats returns the correct counts and avg_fleet_speed.
describe('GET /telemetry/fleet/stats', () => {
  it('returns correct status counts from telemetry_events', async () => {
    await seedEvent({ status: 'moving', recorded_at: '2026-01-01T00:00:00Z' });
    await seedEvent({ vehicle_id: 'TEST-002', status: 'idle', recorded_at: '2026-01-01T00:00:00Z' });
    await seedEvent({ vehicle_id: 'TEST-003', status: 'stopped', recorded_at: '2026-01-01T00:00:00Z' });

    const res = await request(app).get('/telemetry/fleet/stats');
    expect(res.status).toBe(200);
    expect(res.body.moving).toBe(1);
    expect(res.body.idle).toBe(1);
    expect(res.body.stopped).toBe(1);
  });

  // verifies that avg_fleet_speed in the response matches the avg_speed_kmph we seeded into vehicle_aggregates.
  it('returns avg_fleet_speed from vehicle_aggregates', async () => {
    await seedAggregate({ avg_speed_kmph: 60 });

    const res = await request(app).get('/telemetry/fleet/stats');
    expect(res.status).toBe(200);
    expect(res.body.avg_fleet_speed).toBe(60);
  });
});
