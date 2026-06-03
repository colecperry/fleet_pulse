// Unit tests — fast, no infrastructure needed.
// Kafka and the DB are both mocked, so these run anywhere without Docker.
// Only tests one thing: does the route's validation logic accept and reject the right payloads?

import request from 'supertest';
import app from '../index';

// Replace the real Kafka producer with a no-op mock.
// Any test that triggers sendTelemetryEvent won't open a real connection.
jest.mock('../producer', () => ({
  sendTelemetryEvent: jest.fn().mockResolvedValue(undefined),
}));

// Replace the real DB pool so no Postgres connection is needed.
// The mock returns empty results by default; individual tests can override this.
jest.mock('../db', () => ({
  default: { query: jest.fn().mockResolvedValue({ rows: [] }) },
}));

const VALID_PAYLOAD = { // reusable valid payload for tests that passes validation. 
  vehicle_id: 'VH-001',
  lat: 37.7749,
  lon: -122.4194,
  speed_kmph: 45.5,
  status: 'moving',
};

describe('POST /telemetry — validation', () => { // happy path
  it('returns 202 for a valid payload', async () => {
    const res = await request(app).post('/telemetry').send(VALID_PAYLOAD);
    expect(res.status).toBe(202);
  });

  it('returns 400 when vehicle_id is missing', async () => {
    // pull out vehicle_id from the payload, leaving the rest in a new object
    const { vehicle_id, ...payload } = VALID_PAYLOAD;
    const res = await request(app).post('/telemetry').send(payload); // supertest structure
    expect(res.status).toBe(400); // Jest assertion 
  });

  it('returns 400 when lat is missing', async () => {
    const { lat, ...payload } = VALID_PAYLOAD;
    const res = await request(app).post('/telemetry').send(payload);
    expect(res.status).toBe(400);
  });

  it('returns 400 when lon is missing', async () => {
    const { lon, ...payload } = VALID_PAYLOAD;
    const res = await request(app).post('/telemetry').send(payload);
    expect(res.status).toBe(400);
  });

  it('returns 400 when speed_kmph is missing', async () => {
    const { speed_kmph, ...payload } = VALID_PAYLOAD;
    const res = await request(app).post('/telemetry').send(payload);
    expect(res.status).toBe(400);
  });

  it('returns 400 when status is not a valid value', async () => {
    const res = await request(app)
      .post('/telemetry')
      .send({ ...VALID_PAYLOAD, status: 'driving' });
    expect(res.status).toBe(400);
  });

  it('returns 400 when lat is a string instead of a number', async () => {
    const res = await request(app)
      .post('/telemetry')
      .send({ ...VALID_PAYLOAD, lat: '37.7749' });
    expect(res.status).toBe(400);
  });
});
