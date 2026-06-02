import { Pool, QueryResult } from 'pg'

// keeps up to 10 open Postgres connections ready to reuse — cheaper than opening a new one per request
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
})

// single function all other files use to query the database
// params are injected safely into the SQL string to prevent SQL injection
export async function query(text: string, params?: unknown[]): Promise<QueryResult> {
  return pool.query(text, params) // sends the query to Postgres, returns a promise
}

export default pool // export the pool instance so other files can import it