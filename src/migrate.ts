// migrate.ts is a one-shot script — run it once to create tables, then never again
// unless you change the schema. It is NOT the API server; it exits after running.
import dotenv from 'dotenv';
dotenv.config();

import fs from 'fs'; // file-system module in Node.js, used to read the SQL file as a string
import path from 'path'; // path module in Node.js, used to construct the path to the SQL file 
import pool from './db'; // import the pool instance from db.ts to run queries

async function migrate() {
  // __dirname is the directory of this file (src/), so this resolves to src/schema.sql
  const schemaPath = path.join(__dirname, 'schema.sql'); // construct the path to the SQL file
  const sql = fs.readFileSync(schemaPath, 'utf-8'); // read the SQL file as a string

  console.log('Running migration...');

  // Send the entire SQL string to Postgres to create all tables and indexes
  // .query() is a function that takes two args: the query you want to run as a SQL string, and an optional array of parameters, runs the query string in Postgres, and returns a Promise that resolves to a QueryResult object with a .rows property containing the results (array)
  await pool.query(sql);

  console.log('Migration complete.');

  // close all open connections in the pool and exit the script
  await pool.end();
}

// Step 1: Run migrate
if (require.main === module) { // run the migrate function only if this file is executed directly 
  migrate().catch((err) => {
    console.error('Migration failed:', err)
    process.exit(1)
  })
};
