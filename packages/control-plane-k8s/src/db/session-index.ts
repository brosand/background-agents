/**
 * SessionIndexStore — PostgreSQL replacement for Cloudflare D1 SessionIndexStore.
 *
 * Provides listing, filtering, and pagination of sessions.
 */

import type pg from "pg";

export interface SessionEntry {
  id: string;
  title: string | null;
  repoOwner: string;
  repoName: string;
  model: string;
  status: string;
  createdAt: number;
  updatedAt: number;
}

export interface ListSessionsOptions {
  status?: string;
  excludeStatus?: string;
  repoOwner?: string;
  repoName?: string;
  limit?: number;
  offset?: number;
}

export interface ListSessionsResult {
  sessions: SessionEntry[];
  total: number;
  hasMore: boolean;
}

interface SessionRow {
  id: string;
  title: string | null;
  repo_owner: string;
  repo_name: string;
  model: string;
  status: string;
  created_at: string; // PostgreSQL returns BIGINT as string
  updated_at: string;
}

function toEntry(row: SessionRow): SessionEntry {
  return {
    id: row.id,
    title: row.title,
    repoOwner: row.repo_owner,
    repoName: row.repo_name,
    model: row.model,
    status: row.status,
    createdAt: Number(row.created_at),
    updatedAt: Number(row.updated_at),
  };
}

export class SessionIndexStore {
  constructor(private readonly pool: pg.Pool) {}

  async create(session: SessionEntry): Promise<void> {
    await this.pool.query(
      `INSERT INTO sessions (id, title, repo_owner, repo_name, model, status, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (id) DO NOTHING`,
      [
        session.id,
        session.title,
        session.repoOwner.toLowerCase(),
        session.repoName.toLowerCase(),
        session.model,
        session.status,
        session.createdAt,
        session.updatedAt,
      ]
    );
  }

  async get(id: string): Promise<SessionEntry | null> {
    const result = await this.pool.query<SessionRow>(
      "SELECT * FROM sessions WHERE id = $1",
      [id]
    );
    return result.rows[0] ? toEntry(result.rows[0]) : null;
  }

  async list(options: ListSessionsOptions = {}): Promise<ListSessionsResult> {
    const { status, excludeStatus, repoOwner, repoName, limit = 50, offset = 0 } = options;

    const conditions: string[] = [];
    const params: unknown[] = [];
    let paramIdx = 1;

    if (status) {
      conditions.push(`status = $${paramIdx++}`);
      params.push(status);
    }
    if (excludeStatus) {
      conditions.push(`status != $${paramIdx++}`);
      params.push(excludeStatus);
    }
    if (repoOwner) {
      conditions.push(`repo_owner = $${paramIdx++}`);
      params.push(repoOwner.toLowerCase());
    }
    if (repoName) {
      conditions.push(`repo_name = $${paramIdx++}`);
      params.push(repoName.toLowerCase());
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

    const countResult = await this.pool.query<{ count: string }>(
      `SELECT COUNT(*) as count FROM sessions ${where}`,
      params
    );
    const total = Number(countResult.rows[0].count);

    const dataResult = await this.pool.query<SessionRow>(
      `SELECT * FROM sessions ${where} ORDER BY updated_at DESC LIMIT $${paramIdx++} OFFSET $${paramIdx++}`,
      [...params, limit, offset]
    );

    const sessions = dataResult.rows.map(toEntry);

    return {
      sessions,
      total,
      hasMore: offset + sessions.length < total,
    };
  }

  async updateStatus(id: string, status: string): Promise<boolean> {
    const result = await this.pool.query(
      "UPDATE sessions SET status = $1, updated_at = $2 WHERE id = $3",
      [status, Date.now(), id]
    );
    return (result.rowCount ?? 0) > 0;
  }

  async delete(id: string): Promise<boolean> {
    const result = await this.pool.query(
      "DELETE FROM sessions WHERE id = $1",
      [id]
    );
    return (result.rowCount ?? 0) > 0;
  }
}
