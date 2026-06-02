-- This file defines the database schema — the tables and indexes that store our data.
-- migrate.ts reads this file as a plain text string and sends it to Postgres once to create everything.
-- Run once with: npm run migrate

-- enables gen_random_uuid() for auto-generating UUIDs as primary keys
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- stores one row per raw GPS event published by the vehicle simulator
-- every event lands here exactly as received — nothing is modified or aggregated
CREATE TABLE IF NOT EXISTS telemetry_events (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(), -- unique ID auto-generated - each row
  vehicle_id  VARCHAR NOT NULL,                           -- which vehicle sent this event
  lat         DOUBLE PRECISION NOT NULL,                  -- GPS latitude
  lon         DOUBLE PRECISION NOT NULL,                  -- GPS longitude
  speed_kmph  DOUBLE PRECISION NOT NULL,                  -- speed in km/h at time of event
  status      VARCHAR NOT NULL,                           -- 'moving', 'idle', or 'stopped'
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()          -- when the event happened, stored in UTC
);

-- index on vehicle_id + recorded_at so "get latest events for vehicle X" is fast
-- without this, Postgres scans every row in the table to find the right vehicle
-- with it, Postgres jumps directly to that vehicle's rows, already sorted newest-first
CREATE INDEX IF NOT EXISTS idx_telemetry_vehicle_time
  ON telemetry_events (vehicle_id, recorded_at DESC);

-- stores pre-computed 30-second windows written by the Flink job
-- instead of scanning millions of raw events, the API reads aggregated results from here
CREATE TABLE IF NOT EXISTS vehicle_aggregates (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vehicle_id       VARCHAR NOT NULL,
  avg_speed_kmph   DOUBLE PRECISION, -- average speed over the window
  event_count      INTEGER,          -- how many events occurred in the window
  dominant_status  VARCHAR,          -- most common status during the window
  window_start     TIMESTAMPTZ,      -- when the window started
  window_end       TIMESTAMPTZ       -- when the window ended
);

-- same index pattern as telemetry_events — fast lookup of latest windows per vehicle
CREATE INDEX IF NOT EXISTS idx_aggregates_vehicle_window
  ON vehicle_aggregates (vehicle_id, window_start DESC);

