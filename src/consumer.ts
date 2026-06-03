// This file handles consuming telemetry events from Kafka and writing them to Postgres telemetry_events table. It runs indefinitely until the user presses Ctrl+C, which triggers a clean shutdown.
import dotenv from 'dotenv';
dotenv.config();

import { Kafka } from 'kafkajs';
import pool from './db';
import logger from './logger';

// Create a Kafka client instance, which allows us to talk to the Kafka broker (localhost:9092)
const kafka = new Kafka({
  clientId: 'fleet-consumer',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

// Create a consumer instance that belongs to the 'telemetry-service' group.
const consumer = kafka.consumer({ groupId: 'telemetry-service' });

async function run() {
  await consumer.connect(); // open the network connection

  // tell the consumer to listen to the 'telemetry' topic. On first run, read all events from the beginning of the topic (offset 0). On subsequent runs, Kafka ignores fromBeginning and uses the committed offset for this group, so we pick up where we left off.
  await consumer.subscribe({ topic: 'telemetry', fromBeginning: true });

  // start the consumer loop. eachMessage() is a callback that runs for each message received.
  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      const event = JSON.parse(message.value.toString()); // convert bytes back to str, then to JSON, then to a TelemetryEvent object
      
      // Insert the event into the telemetry_events table in Postgres. The placeholders ($1, $2) are replaced by the vals in the array in the second arg to prevent SQL injection
      await pool.query(
        `INSERT INTO telemetry_events (vehicle_id, lat, lon, speed_kmph, status, recorded_at)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [
          event.vehicle_id,
          event.lat,
          event.lon,
          event.speed_kmph,
          event.status,
          event.recorded_at,
        ]
      );

      logger.info({ vehicle_id: event.vehicle_id, speed_kmph: event.speed_kmph }, 'event persisted');
    },
  });
}

// Disconnect cleanly on Ctrl+C so Kafka knows this consumer left the group
// and can reassign its partitions to other members immediately.
process.on('SIGINT', async () => {
  logger.info('shutting down consumer');
  await consumer.disconnect();
  process.exit(0);
});

// Start the consumer
run().catch((err) => {
  logger.error({ err }, 'consumer error');
  process.exit(1);
});
