// Load .env first so TEST_DATABASE_URL is available, then swap DATABASE_URL.
// setupFiles runs before any test module loads — dotenv must be called here
// because index.ts hasn't been imported yet, so its dotenv.config() hasn't run.
import dotenv from 'dotenv';
dotenv.config();

if (process.env.TEST_DATABASE_URL) {
  process.env.DATABASE_URL = process.env.TEST_DATABASE_URL;
}
