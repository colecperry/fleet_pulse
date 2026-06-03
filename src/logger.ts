import pino from 'pino';

// In development, pipe through pino-pretty for human-readable output.
// In production, emit raw JSON so log aggregators (Datadog, CloudWatch) can parse it.
const logger = pino({ // create a logger instance
  level: process.env.LOG_LEVEL || 'info',
  ...(process.env.NODE_ENV !== 'production' && {
    transport: {
      target: 'pino-pretty',
      options: { colorize: true },
    },
  }),
});

export default logger;
