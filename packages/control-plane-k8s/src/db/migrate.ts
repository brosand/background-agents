/**
 * Run PostgreSQL schema migration on startup.
 *
 * Reads schema.sql and executes it against the database.
 */

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { getPool, closePool } from "./pool.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

export async function migrate(): Promise<void> {
  const schemaPath = join(__dirname, "schema.sql");
  const sql = readFileSync(schemaPath, "utf-8");
  const pool = getPool();

  await pool.query(sql);
  console.log("Database migration completed");
}

// Run directly when executed as a script
if (process.argv[1] && process.argv[1].endsWith("migrate.js")) {
  migrate()
    .then(() => closePool())
    .catch((err) => {
      console.error("Migration failed:", err);
      process.exit(1);
    });
}
