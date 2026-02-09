/**
 * RepoSecretsStore — PostgreSQL replacement for Cloudflare D1 RepoSecretsStore.
 *
 * Secrets are encrypted with AES-256-GCM before storage, using the same
 * crypto module as the Cloudflare version (Web Crypto API is available in
 * Node.js 20+).
 */

import type pg from "pg";
import { encryptToken, decryptToken } from "../auth/crypto.js";

const VALID_KEY_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;
const MAX_KEY_LENGTH = 256;
const MAX_VALUE_SIZE = 16384;
const MAX_TOTAL_VALUE_SIZE = 65536;
const MAX_SECRETS_PER_REPO = 50;

const RESERVED_KEYS = new Set([
  "PYTHONUNBUFFERED",
  "SANDBOX_ID",
  "CONTROL_PLANE_URL",
  "SANDBOX_AUTH_TOKEN",
  "REPO_OWNER",
  "REPO_NAME",
  "GITHUB_APP_TOKEN",
  "SESSION_CONFIG",
  "RESTORED_FROM_SNAPSHOT",
  "OPENCODE_CONFIG_CONTENT",
  "PATH",
  "HOME",
  "USER",
  "SHELL",
  "TERM",
  "PWD",
  "LANG",
]);

export class RepoSecretsValidationError extends Error {}

export interface SecretMetadata {
  key: string;
  createdAt: number;
  updatedAt: number;
}

export class RepoSecretsStore {
  constructor(
    private readonly pool: pg.Pool,
    private readonly encryptionKey: string
  ) {}

  normalizeKey(key: string): string {
    return key.toUpperCase();
  }

  validateKey(key: string): void {
    if (!key || key.length > MAX_KEY_LENGTH)
      throw new RepoSecretsValidationError("Key too long or empty");
    if (!VALID_KEY_PATTERN.test(key))
      throw new RepoSecretsValidationError("Key must match [A-Za-z_][A-Za-z0-9_]*");
    if (RESERVED_KEYS.has(key.toUpperCase()))
      throw new RepoSecretsValidationError(`Key '${key}' is reserved`);
  }

  validateValue(value: string): void {
    if (typeof value !== "string") throw new RepoSecretsValidationError("Value must be a string");
    const bytes = new TextEncoder().encode(value).length;
    if (bytes > MAX_VALUE_SIZE)
      throw new RepoSecretsValidationError(`Value exceeds ${MAX_VALUE_SIZE} bytes`);
  }

  async setSecrets(
    repoId: number,
    repoOwner: string,
    repoName: string,
    secrets: Record<string, string>
  ): Promise<{ created: number; updated: number; keys: string[] }> {
    const owner = repoOwner.toLowerCase();
    const name = repoName.toLowerCase();
    const now = Date.now();

    const normalized: Record<string, string> = {};
    let totalValueBytes = 0;
    for (const [rawKey, value] of Object.entries(secrets)) {
      const key = this.normalizeKey(rawKey);
      this.validateKey(key);
      this.validateValue(value);
      totalValueBytes += new TextEncoder().encode(value).length;
      normalized[key] = value;
    }

    if (totalValueBytes > MAX_TOTAL_VALUE_SIZE) {
      throw new RepoSecretsValidationError(
        `Total secret size exceeds ${MAX_TOTAL_VALUE_SIZE} bytes`
      );
    }

    const existingResult = await this.pool.query<{ key: string }>(
      "SELECT key FROM repo_secrets WHERE repo_id = $1",
      [repoId]
    );
    const existingKeySet = new Set(existingResult.rows.map((r) => r.key));

    const incomingKeys = Object.keys(normalized);
    const netNew = incomingKeys.filter((k) => !existingKeySet.has(k)).length;
    if (existingKeySet.size + netNew > MAX_SECRETS_PER_REPO) {
      throw new RepoSecretsValidationError(
        `Repository would exceed ${MAX_SECRETS_PER_REPO} secrets limit ` +
          `(current: ${existingKeySet.size}, adding: ${netNew})`
      );
    }

    let created = 0;
    let updated = 0;

    // Use a transaction for atomicity
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      for (const [key, value] of Object.entries(normalized)) {
        const encrypted = await encryptToken(value, this.encryptionKey);
        const isNew = !existingKeySet.has(key);
        if (isNew) created++;
        else updated++;

        await client.query(
          `INSERT INTO repo_secrets (repo_id, repo_owner, repo_name, key, encrypted_value, created_at, updated_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (repo_id, key) DO UPDATE SET
             repo_owner = EXCLUDED.repo_owner,
             repo_name = EXCLUDED.repo_name,
             encrypted_value = EXCLUDED.encrypted_value,
             updated_at = EXCLUDED.updated_at`,
          [repoId, owner, name, key, encrypted, now, now]
        );
      }
      await client.query("COMMIT");
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }

    return { created, updated, keys: incomingKeys };
  }

  async listSecretKeys(repoId: number): Promise<SecretMetadata[]> {
    const result = await this.pool.query<{ key: string; created_at: string; updated_at: string }>(
      "SELECT key, created_at, updated_at FROM repo_secrets WHERE repo_id = $1 ORDER BY key",
      [repoId]
    );

    return result.rows.map((row) => ({
      key: row.key,
      createdAt: Number(row.created_at),
      updatedAt: Number(row.updated_at),
    }));
  }

  async getDecryptedSecrets(repoId: number): Promise<Record<string, string>> {
    const result = await this.pool.query<{ key: string; encrypted_value: string }>(
      "SELECT key, encrypted_value FROM repo_secrets WHERE repo_id = $1",
      [repoId]
    );

    const secrets: Record<string, string> = {};
    for (const row of result.rows) {
      secrets[row.key] = await decryptToken(row.encrypted_value, this.encryptionKey);
    }

    return secrets;
  }

  async deleteSecret(repoId: number, key: string): Promise<boolean> {
    const result = await this.pool.query(
      "DELETE FROM repo_secrets WHERE repo_id = $1 AND key = $2",
      [repoId, this.normalizeKey(key)]
    );
    return (result.rowCount ?? 0) > 0;
  }
}
