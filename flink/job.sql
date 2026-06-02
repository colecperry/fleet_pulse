-- Fleet Pulse — Flink SQL streaming job
-- Reads raw telemetry events from Kafka, computes 30-second rolling aggregations per vehicle, and writes the results to PostgreSQL.
--
-- Submit this file to the Flink SQL client:
--   docker exec -it fleet_pulse-flink-jobmanager-1 /opt/flink/bin/sql-client.sh -f /job.sql

-- Defines a virtual table that maps to the Kafka topic. Flink reads from here — every Kafka message becomes a row.
CREATE TABLE telemetry_source (
    vehicle_id  STRING,
    lat         DOUBLE,
    lon         DOUBLE,
    speed_kmph  DOUBLE,
    status      STRING,
    recorded_at STRING,
    -- Converts the recorded_at string into an actual timestamp Flink can use for windowing.
    event_time  AS TO_TIMESTAMP(recorded_at, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''),
    -- Tells Flink to wait 5 seconds for late-arriving events before closing a window. Some leniency for out-of-order events - could be due to network delays or clock skew.
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND

) WITH ( -- Connects this virtual table to the Kafka topic
    'connector'                    = 'kafka',
    'topic'                        = 'telemetry',
    -- Inside Docker, services reach Kafka on port 29092 (the internal listener).
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id'          = 'flink-telemetry-job',
    -- Read from the beginning of the topic so no events are missed on job start.
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json'
);

-- Sink table: virtual table that maps to the PostgreSQL aggregates table. Flink writes results here - column names must match the vehicle_aggregates table in schema.sql.
CREATE TABLE vehicle_aggregates_sink (
    vehicle_id      STRING,
    avg_speed_kmph  DOUBLE,
    event_count     BIGINT,
    window_start    TIMESTAMP(3),
    window_end      TIMESTAMP(3)
) WITH (
    -- connects this virtual table to the PostgreSQL database and table via JDBC. Flink will execute INSERT statements here and write results to the vehicle_aggregates table.
    'connector'  = 'jdbc',
    'url'        = 'jdbc:postgresql://postgres:5432/fleet_pulse',
    'table-name' = 'vehicle_aggregates',
    'username'   = 'fleet',
    'password'   = 'fleet',
    'driver'     = 'org.postgresql.Driver'
);

-- Reads raw events from Kafka, groups them into 30-second buckets per vehicle, and writes one summary row per bucket to PostgreSQL.
-- TUMBLE = non-overlapping fixed time buckets (0-30s, 30-60s, 60-90s...) — each bucket closes and a row is written once the window ends.
INSERT INTO vehicle_aggregates_sink             -- 5. write results to PostgreSQL
SELECT
    vehicle_id,               
    -- 4. calc avg speed & event count per bucket                                             
    AVG(speed_kmph)                                AS avg_speed_kmph, 
    COUNT(*)                                       AS event_count,         
    TUMBLE_START(event_time, INTERVAL '30' SECOND) AS window_start,        
    TUMBLE_END(event_time, INTERVAL '30' SECOND)   AS window_end           
FROM telemetry_source                           -- 1. read from Kafka
GROUP BY
    vehicle_id,                                 -- 2+3. group into 30s buckets per vehicle
    TUMBLE(event_time, INTERVAL '30' SECOND);      
