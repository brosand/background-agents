/**
 * RepoMetadataStore — PostgreSQL replacement for Cloudflare D1 RepoMetadataStore.
 */

import type pg from "pg";
import type { RepoMetadata } from "@open-inspect/shared";

interface RepoMetadataRow {
  repo_owner: string;
  repo_name: string;
  description: string | null;
  aliases: string[] | null;
  channel_associations: string[] | null;
  keywords: string[] | null;
  created_at: string;
  updated_at: string;
}

function toMetadata(row: RepoMetadataRow): RepoMetadata {
  const metadata: RepoMetadata = {};
  if (row.description != null) metadata.description = row.description;
  if (row.aliases) metadata.aliases = row.aliases;
  if (row.channel_associations) metadata.channelAssociations = row.channel_associations;
  if (row.keywords) metadata.keywords = row.keywords;
  return metadata;
}

export class RepoMetadataStore {
  constructor(private readonly pool: pg.Pool) {}

  async get(owner: string, name: string): Promise<RepoMetadata | null> {
    const result = await this.pool.query<RepoMetadataRow>(
      "SELECT * FROM repo_metadata WHERE repo_owner = $1 AND repo_name = $2",
      [owner.toLowerCase(), name.toLowerCase()]
    );
    return result.rows[0] ? toMetadata(result.rows[0]) : null;
  }

  async upsert(owner: string, name: string, metadata: RepoMetadata): Promise<void> {
    const now = Date.now();
    await this.pool.query(
      `INSERT INTO repo_metadata (repo_owner, repo_name, description, aliases, channel_associations, keywords, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (repo_owner, repo_name) DO UPDATE SET
         description = EXCLUDED.description,
         aliases = EXCLUDED.aliases,
         channel_associations = EXCLUDED.channel_associations,
         keywords = EXCLUDED.keywords,
         updated_at = EXCLUDED.updated_at`,
      [
        owner.toLowerCase(),
        name.toLowerCase(),
        metadata.description ?? null,
        metadata.aliases ? JSON.stringify(metadata.aliases) : null,
        metadata.channelAssociations ? JSON.stringify(metadata.channelAssociations) : null,
        metadata.keywords ? JSON.stringify(metadata.keywords) : null,
        now,
        now,
      ]
    );
  }

  async getBatch(
    repos: Array<{ owner: string; name: string }>
  ): Promise<Map<string, RepoMetadata>> {
    if (repos.length === 0) return new Map();

    const map = new Map<string, RepoMetadata>();

    // Build a single query with ANY() for efficiency
    const owners = repos.map((r) => r.owner.toLowerCase());
    const names = repos.map((r) => r.name.toLowerCase());

    // Use a CTE with unnest to do a single round-trip
    const result = await this.pool.query<RepoMetadataRow>(
      `SELECT rm.* FROM repo_metadata rm
       INNER JOIN unnest($1::text[], $2::text[]) AS t(owner, name)
       ON rm.repo_owner = t.owner AND rm.repo_name = t.name`,
      [owners, names]
    );

    for (const row of result.rows) {
      const key = `${row.repo_owner}/${row.repo_name}`;
      map.set(key, toMetadata(row));
    }

    return map;
  }
}
