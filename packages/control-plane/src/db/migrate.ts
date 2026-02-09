/**
 * PostgreSQL migration runner.
 *
 * Applies schema.sql on startup. Idempotent via IF NOT EXISTS.
 */

import * as fs from "node:fs";
import * as path from "node:path";
import { getPool } from "./postgres";
import { createLogger } from "../logger";

const log = createLogger("migrate");

/**
 * Run database migrations.
 *
 * Reads schema.sql and executes it. All statements use IF NOT EXISTS
 * or ON CONFLICT so they are idempotent.
 */
export async function runMigrations(): Promise<void> {
  const schemaPath = path.join(__dirname, "schema.sql");
  const schema = fs.readFileSync(schemaPath, "utf-8");

  const pool = getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    await client.query(schema);
    await client.query("COMMIT");
    log.info("Migrations applied successfully");
  } catch (e) {
    await client.query("ROLLBACK");
    log.error("Migration failed", { error: e instanceof Error ? e.message : String(e) });
    throw e;
  } finally {
    client.release();
  }
}
