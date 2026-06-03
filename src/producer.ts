// producer.ts — publishes telemetry events to Kafka on behalf of POST /telemetry.
//
// Events are dropped into Kafka rather than written to PostgreSQL directly.
// The consumer handles persistence independently, decoupling ingestion from storage.
//
// Flow: simulator → POST /telemetry → producer.ts → Kafka → consumer.ts → PostgreSQL

import { Kafka, Producer } from 'kafkajs';

// Defines the shape of a telemetry events (exportable) - mirrors the columns in our PostgreSQL table telemetry_events
export interface TelemetryEvent {
  vehicle_id: string;
  lat: number;
  lon: number;
  speed_kmph: number;
  status: 'moving' | 'idle' | 'stopped';
  recorded_at: string;
}

// Create a Kafka client instance, which allows us to talk to the Kafka broker (localhost:9092)
const kafka = new Kafka({ 
  clientId: 'fleet-api',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

// Module-level producer instance (starts at null) — created once on getProducer() and reused across all calls to sendTelemetryEvent. Opening a new producer per request would be expensive
let producer: Producer | null = null;

// Lazy connect — the producer is created and connected on the first call, then reused for every subsequent call. 
async function getProducer(): Promise<Producer> {
  if (!producer) {
    producer = kafka.producer(); // create a producer instance
    await producer.connect(); // establish the connection to the Kafka broker
  }
  return producer; // return the connected producer instance for sending messages
}

// Kafka expects: { topic, messages: [{ key, value }] }
// key = vehicle_id so all events from the same vehicle go to the same partition — preserving event order
// value = JSON string of the event (array so we could send multiple events at once)
export async function sendTelemetryEvent(event: TelemetryEvent): Promise<void> {
  const p = await getProducer(); // create the producer, promise resolves to a Producer
  await p.send({ // await to make sure the message lands in Kafka before we move, which is a Promise that resolves to nothing
    topic: 'telemetry',
    messages: [{ key: event.vehicle_id, value: JSON.stringify(event) }],
  });
}

// Called on server shutdown to close the Kafka connection cleanly. Async because closing a network connection takes time, and we want to wait for it to finish before exiting the process.
export async function disconnectProducer(): Promise<void> {
  if (producer) {
    await producer.disconnect(); // closes the connection to the Kafka broker
    producer = null; // turns the producer back to null so if we start the simulator again, it will create a fresh connection
  }
}
