/**
 * PostgreSQL repository secrets store.
 *
 * Replaces RepoSecretsStore (D1-backed) with PostgreSQL queries.
 * Secrets are encrypted with AES-256-GCM before storage.
 */

import { query, queryOne, execute, transaction } from "./postgres";
import { encryptToken, decryptToken } from "../auth/crypto";

/** System environment variable keys that cannot be overridden by user secrets. */
const RESERVED_KEYS = new Set([
  "SANDBOX_ID",
  "CONTROL_PLANE_URL",
  "SANDBOX_AUTH_TOKEN",
  "REPO_OWNER",
  "REPO_NAME",
  "SESSION_CONFIG",
  "GITHUB_APP_TOKEN",
  "RESTORED_FROM_SNAPSHOT",
  "PYTHONUNBUFFERED",
  "PATH",
  "HOME",
  "NODE_ENV",
  "NODE_PATH",
  "PYTHONPATH",
]);

const KEY_REGEX = /^[A-Za-z_][A-Za-z0-9_]*$/;
const MAX_VALUE_SIZE = 16 * 1024; // 16 KB per value
const MAX_TOTAL_SIZE = 64 * 1024; // 64 KB total per repo
const MAX_SECRETS_PER_REPO = 50;

export class RepoSecretsValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RepoSecretsValidationError";
  }
}

export class PgRepoSecretsStore {
  constructor(private readonly encryptionKey: string) {}

  async setSecrets(
    repoId: string,
    repoOwner: string,
    repoName: string,
    secrets: Record<string, string>
  ): Promise<void> {
    const entries = Object.entries(secrets);

    // Validate
    let totalSize = 0;
    for (const [key, value] of entries) {
      if (!KEY_REGEX.test(key)) {
        throw new RepoSecretsValidationError(
          `Invalid key "${key}": must match ${KEY_REGEX}`
        );
      }
      if (RESERVED_KEYS.has(key)) {
        throw new RepoSecretsValidationError(`Reserved key "${key}" cannot be set`);
      }
      if (value.length > MAX_VALUE_SIZE) {
        throw new RepoSecretsValidationError(
          `Value for "${key}" exceeds ${MAX_VALUE_SIZE} bytes`
        );
      }
      totalSize += value.length;
    }

    if (totalSize > MAX_TOTAL_SIZE) {
      throw new RepoSecretsValidationError(
        `Total value size exceeds ${MAX_TOTAL_SIZE} bytes`
      );
    }

    // Check per-repo limit
    const existing = await queryOne<{ count: string }>(
      `SELECT COUNT(*) as count FROM repo_secrets WHERE repo_owner = $1 AND repo_name = $2`,
      [repoOwner, repoName]
    );
    const existingCount = parseInt(existing?.count ?? "0", 10);
    const newKeys = entries.filter(
      ([key]) => !entries.find(([k]) => k === key)
    ).length;
    if (existingCount + newKeys > MAX_SECRETS_PER_REPO) {
      throw new RepoSecretsValidationError(
        `Maximum ${MAX_SECRETS_PER_REPO} secrets per repo`
      );
    }

    const now = Date.now();

    await transaction(async (client) => {
      for (const [key, value] of entries) {
        const encrypted = await encryptToken(value, this.encryptionKey);
        await client.query(
          `INSERT INTO repo_secrets (repo_owner, repo_name, key, value_encrypted, repo_id, updated_at)
           VALUES ($1, $2, $3, $4, $5, $6)
           ON CONFLICT (repo_owner, repo_name, key) DO UPDATE SET
             value_encrypted = EXCLUDED.value_encrypted,
             updated_at = EXCLUDED.updated_at`,
          [repoOwner, repoName, key, encrypted, repoId, now]
        );
      }
    });
  }

  async listSecretKeys(
    repoOwner: string,
    repoName: string
  ): Promise<Array<{ key: string; updated_at: number }>> {
    return query<{ key: string; updated_at: number }>(
      `SELECT key, updated_at FROM repo_secrets WHERE repo_owner = $1 AND repo_name = $2 ORDER BY key`,
      [repoOwner, repoName]
    );
  }

  async getDecryptedSecrets(
    repoOwner: string,
    repoName: string
  ): Promise<Record<string, string>> {
    const rows = await query<{ key: string; value_encrypted: string }>(
      `SELECT key, value_encrypted FROM repo_secrets WHERE repo_owner = $1 AND repo_name = $2`,
      [repoOwner, repoName]
    );

    const result: Record<string, string> = {};
    for (const row of rows) {
      try {
        result[row.key] = await decryptToken(row.value_encrypted, this.encryptionKey);
      } catch {
        // Skip secrets that fail to decrypt (key rotation, corruption)
      }
    }
    return result;
  }

  async deleteSecret(repoOwner: string, repoName: string, key: string): Promise<void> {
    await execute(
      `DELETE FROM repo_secrets WHERE repo_owner = $1 AND repo_name = $2 AND key = $3`,
      [repoOwner, repoName, key]
    );
  }
}
