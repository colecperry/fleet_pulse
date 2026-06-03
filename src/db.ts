import { Pool } from 'pg' // PostgreSQL client for Node.js

// keeps up to 10 open Postgres connections ready to reuse — cheaper than opening a new one per request
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
})

export default pool // export the pool instance so other files can import it