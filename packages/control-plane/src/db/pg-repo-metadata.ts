/**
 * PostgreSQL repository metadata store.
 *
 * Replaces RepoMetadataStore (D1-backed) with PostgreSQL queries.
 */

import { query, queryOne, execute } from "./postgres";

export interface RepoMetadata {
  description?: string;
  aliases?: string[];
  channel_association?: string;
  keywords?: string[];
}

interface RepoMetadataRow {
  repo_owner: string;
  repo_name: string;
  description: string | null;
  aliases: string | null;
  channel_association: string | null;
  keywords: string | null;
  updated_at: number | null;
}

export class PgRepoMetadataStore {
  async get(owner: string, name: string): Promise<RepoMetadata | null> {
    const row = await queryOne<RepoMetadataRow>(
      `SELECT * FROM repo_metadata WHERE repo_owner = $1 AND repo_name = $2`,
      [owner, name]
    );

    if (!row) return null;
    return this.rowToMetadata(row);
  }

  async upsert(owner: string, name: string, metadata: RepoMetadata): Promise<void> {
    const now = Date.now();
    await execute(
      `INSERT INTO repo_metadata (repo_owner, repo_name, description, aliases, channel_association, keywords, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (repo_owner, repo_name) DO UPDATE SET
         description = EXCLUDED.description,
         aliases = EXCLUDED.aliases,
         channel_association = EXCLUDED.channel_association,
         keywords = EXCLUDED.keywords,
         updated_at = EXCLUDED.updated_at`,
      [
        owner,
        name,
        metadata.description ?? null,
        metadata.aliases ? JSON.stringify(metadata.aliases) : null,
        metadata.channel_association ?? null,
        metadata.keywords ? JSON.stringify(metadata.keywords) : null,
        now,
      ]
    );
  }

  async getBatch(repos: Array<{ owner: string; name: string }>): Promise<Map<string, RepoMetadata>> {
    if (repos.length === 0) return new Map();

    // Build a single query with ANY for efficient batch lookup
    const owners = repos.map((r) => r.owner);
    const names = repos.map((r) => r.name);

    const rows = await query<RepoMetadataRow>(
      `SELECT * FROM repo_metadata
       WHERE (repo_owner, repo_name) IN (
         SELECT unnest($1::text[]), unnest($2::text[])
       )`,
      [owners, names]
    );

    const map = new Map<string, RepoMetadata>();
    for (const row of rows) {
      map.set(`${row.repo_owner}/${row.repo_name}`, this.rowToMetadata(row));
    }
    return map;
  }

  private rowToMetadata(row: RepoMetadataRow): RepoMetadata {
    return {
      description: row.description ?? undefined,
      aliases: row.aliases ? JSON.parse(row.aliases) : undefined,
      channel_association: row.channel_association ?? undefined,
      keywords: row.keywords ? JSON.parse(row.keywords) : undefined,
    };
  }
}
