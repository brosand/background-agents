/**
 * PostgreSQL connection pool for Open-Inspect Control Plane.
 *
 * Replaces Cloudflare D1 database bindings with standard PostgreSQL.
 */

import pg from "pg";
import { createLogger } from "../logger";

const logger = createLogger("postgres");

let pool: pg.Pool | null = null;

/**
 * Get or create the PostgreSQL connection pool.
 */
export function getPool(databaseUrl: string): pg.Pool {
  if (!pool) {
    pool = new pg.Pool({
      connectionString: databaseUrl,
      max: 20,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 5_000,
    });

    pool.on("error", (err) => {
      logger.error("Unexpected pool error", { error: err });
    });

    logger.info("PostgreSQL pool created");
  }
  return pool;
}

/**
 * Close the connection pool (for graceful shutdown).
 */
export async function closePool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
    logger.info("PostgreSQL pool closed");
  }
}

/**
 * Execute a parameterized query.
 */
export async function query<T extends pg.QueryResultRow = pg.QueryResultRow>(
  p: pg.Pool,
  text: string,
  params?: unknown[]
): Promise<pg.QueryResult<T>> {
  const start = Date.now();
  const result = await p.query<T>(text, params);
  const durationMs = Date.now() - start;

  if (durationMs > 500) {
    logger.warn("Slow query", {
      duration_ms: durationMs,
      rows: result.rowCount,
      query: text.slice(0, 100),
    });
  }

  return result;
}

/**
 * Execute a function within a transaction.
 */
export async function transaction<T>(
  p: pg.Pool,
  fn: (client: pg.PoolClient) => Promise<T>
): Promise<T> {
  const client = await p.getClient();
  try {
    await client.query("BEGIN");
    const result = await fn(client);
    await client.query("COMMIT");
    return result;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}
