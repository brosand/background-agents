/**
 * PostgreSQL client abstraction.
 *
 * Replaces Cloudflare D1 and Durable Object SQLite storage.
 * Provides a unified database layer for all control plane state.
 */

import pg from "pg";

const { Pool } = pg;

export interface PgConfig {
  connectionString?: string;
  host?: string;
  port?: number;
  database?: string;
  user?: string;
  password?: string;
  ssl?: boolean;
  maxConnections?: number;
}

let pool: pg.Pool | null = null;

/**
 * Get or create the PostgreSQL connection pool.
 */
export function getPool(config?: PgConfig): pg.Pool {
  if (pool) return pool;

  const connectionString =
    config?.connectionString || process.env.DATABASE_URL;

  if (connectionString) {
    pool = new Pool({
      connectionString,
      max: config?.maxConnections ?? 20,
      ssl: config?.ssl !== false ? { rejectUnauthorized: false } : undefined,
    });
  } else {
    pool = new Pool({
      host: config?.host || process.env.PGHOST || "localhost",
      port: config?.port || parseInt(process.env.PGPORT || "5432"),
      database: config?.database || process.env.PGDATABASE || "open_inspect",
      user: config?.user || process.env.PGUSER || "postgres",
      password: config?.password || process.env.PGPASSWORD,
      max: config?.maxConnections ?? 20,
    });
  }

  return pool;
}

/**
 * Execute a query and return rows.
 */
export async function query<T = Record<string, unknown>>(
  sql: string,
  params?: unknown[]
): Promise<T[]> {
  const p = getPool();
  const result = await p.query(sql, params);
  return result.rows as T[];
}

/**
 * Execute a query and return the first row or null.
 */
export async function queryOne<T = Record<string, unknown>>(
  sql: string,
  params?: unknown[]
): Promise<T | null> {
  const rows = await query<T>(sql, params);
  return rows[0] ?? null;
}

/**
 * Execute a statement (INSERT, UPDATE, DELETE) and return affected row count.
 */
export async function execute(sql: string, params?: unknown[]): Promise<number> {
  const p = getPool();
  const result = await p.query(sql, params);
  return result.rowCount ?? 0;
}

/**
 * Run a function within a transaction.
 */
export async function transaction<T>(fn: (client: pg.PoolClient) => Promise<T>): Promise<T> {
  const p = getPool();
  const client = await p.connect();
  try {
    await client.query("BEGIN");
    const result = await fn(client);
    await client.query("COMMIT");
    return result;
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

/**
 * Shutdown the connection pool.
 */
export async function shutdown(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
  }
}
