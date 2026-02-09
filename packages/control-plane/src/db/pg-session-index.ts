/**
 * PostgreSQL session index store.
 *
 * Replaces SessionIndexStore (D1-backed) with PostgreSQL queries.
 * Provides the same listing/filtering interface for the dashboard.
 */

import { query, queryOne, execute } from "./postgres";

export interface SessionEntry {
  id: string;
  owner_user_id: string;
  title: string | null;
  repo_owner: string;
  repo_name: string;
  repo_id: number | null;
  status: string;
  created_at: number;
  updated_at: number;
}

export interface ListSessionsOptions {
  ownerUserId?: string;
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
}

export class PgSessionIndexStore {
  async create(session: SessionEntry): Promise<void> {
    await execute(
      `INSERT INTO sessions (id, owner_user_id, title, repo_owner, repo_name, repo_id, status, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
       ON CONFLICT (id) DO NOTHING`,
      [
        session.id,
        session.owner_user_id,
        session.title,
        session.repo_owner,
        session.repo_name,
        session.repo_id,
        session.status,
        session.created_at,
        session.updated_at,
      ]
    );
  }

  async get(id: string): Promise<SessionEntry | null> {
    return queryOne<SessionEntry>(
      `SELECT id, owner_user_id, title, repo_owner, repo_name, repo_id, status, created_at, updated_at
       FROM sessions WHERE id = $1`,
      [id]
    );
  }

  async list(options: ListSessionsOptions = {}): Promise<ListSessionsResult> {
    const conditions: string[] = [];
    const params: unknown[] = [];
    let paramIdx = 1;

    if (options.ownerUserId) {
      conditions.push(`owner_user_id = $${paramIdx++}`);
      params.push(options.ownerUserId);
    }
    if (options.status) {
      conditions.push(`status = $${paramIdx++}`);
      params.push(options.status);
    }
    if (options.excludeStatus) {
      conditions.push(`status != $${paramIdx++}`);
      params.push(options.excludeStatus);
    }
    if (options.repoOwner) {
      conditions.push(`repo_owner = $${paramIdx++}`);
      params.push(options.repoOwner);
    }
    if (options.repoName) {
      conditions.push(`repo_name = $${paramIdx++}`);
      params.push(options.repoName);
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
    const limit = options.limit ?? 50;
    const offset = options.offset ?? 0;

    const [sessions, countResult] = await Promise.all([
      query<SessionEntry>(
        `SELECT id, owner_user_id, title, repo_owner, repo_name, repo_id, status, created_at, updated_at
         FROM sessions ${where}
         ORDER BY updated_at DESC
         LIMIT $${paramIdx++} OFFSET $${paramIdx++}`,
        [...params, limit, offset]
      ),
      queryOne<{ count: string }>(
        `SELECT COUNT(*) as count FROM sessions ${where}`,
        params
      ),
    ]);

    return {
      sessions,
      total: parseInt(countResult?.count ?? "0", 10),
    };
  }

  async updateStatus(id: string, status: string): Promise<void> {
    await execute(
      `UPDATE sessions SET status = $1, updated_at = $2 WHERE id = $3`,
      [status, Date.now(), id]
    );
  }

  async delete(id: string): Promise<void> {
    await execute(`DELETE FROM sessions WHERE id = $1`, [id]);
  }
}
