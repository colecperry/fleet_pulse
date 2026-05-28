// dotenv reads the .env file and injects its key=value pairs into process.env (node's built-in object for environment variables). 
import dotenv from 'dotenv';
dotenv.config(); // This must be called before any code that reads from process.env.

import express from 'express';

const app = express(); // create an Express application instance. This is the main object we use to define routes and middleware.

// express.json() is middleware that parses incoming request bodies as JSON.
// Without it, req.body would always be undefined for POST/PUT requests with JSON payloads.
app.use(express.json());

const PORT = process.env.PORT || 3000; // Use the PORT from environment variables, or default to 3000 if not set.

// /health is a standard liveness endpoint — load balancers and container orchestrators
// (Kubernetes, ECS) ping this to check if the service is alive before routing traffic to it.
app.get('/health', (_req, res) => {
  res.json({ status: 'ok' });
});

app.listen(PORT, () => {
  console.log(`Fleet Pulse API listening on port ${PORT}`);
});

export default app;
