/**
 * API router for Open-Inspect Control Plane (Hono / Node.js).
 *
 * Replaces the Cloudflare Workers custom router with Hono routes.
 * Uses PostgreSQL directly instead of D1 and Durable Objects.
 */

import { Hono } from "hono";
import { cors } from "hono/cors";
import type { Pool } from "pg";
import { generateId, encryptToken } from "./auth/crypto";
import { verifyInternalToken } from "./auth/internal";
import {
  getGitHubAppConfig,
  getInstallationRepository,
  listInstallationRepositories,
} from "./auth/github-app";
import { createLogger } from "./logger";
import type { Config } from "./config";
import type { SessionManager } from "./session/manager";
import type { RivetSandboxProvider } from "./sandbox/providers/rivet-provider";
import type { RedisCache } from "./db/redis";
import type {
  EnrichedRepository,
  InstallationRepository,
  RepoMetadata,
} from "@open-inspect/shared";

const logger = createLogger("router");

const REPOS_CACHE_KEY = "repos:list";
const REPOS_CACHE_FRESH_MS = 5 * 60 * 1000;
const REPOS_CACHE_TTL_SECONDS = 3600;

interface CachedReposList {
  repos: EnrichedRepository[];
  cachedAt: string;
  freshUntil?: number;
}

/**
 * Create the Hono application with all routes.
 */
export function createRouter(
  pool: Pool,
  redis: RedisCache,
  sandboxProvider: RivetSandboxProvider,
  sessionManager: SessionManager,
  config: Config
): Hono {
  const app = new Hono();

  // CORS middleware
  app.use("*", cors());

  // Request ID middleware
  app.use("*", async (c, next) => {
    const requestId = crypto.randomUUID().slice(0, 8);
    const traceId = c.req.header("x-trace-id") || crypto.randomUUID();
    c.header("x-request-id", requestId);
    c.header("x-trace-id", traceId);
    await next();
  });

  // Auth middleware helper
  async function requireAuth(c: { req: { header: (name: string) => string | undefined }; json: (data: unknown, status?: number) => Response }): Promise<Response | null> {
    if (!config.internalCallbackSecret) {
      return c.json({ error: "Internal authentication not configured" }, 500);
    }

    const isValid = await verifyInternalToken(
      c.req.header("Authorization") || null,
      config.internalCallbackSecret
    );

    if (!isValid) {
      return c.json({ error: "Unauthorized" }, 401);
    }

    return null;
  }

  // Health check (public)
  app.get("/health", (c) =>
    c.json({
      status: "healthy",
      service: "open-inspect-control-plane",
      provider: "kubernetes",
    })
  );

  // ---- Session routes ----

  // List sessions
  app.get("/sessions", async (c) => {
    const authErr = await requireAuth(c);
    if (authErr) return authErr;

    const limit = Math.min(parseInt(c.req.query("limit") || "50"), 100);
    const offset = parseInt(c.req.query("offset") || "0");
    const status = c.req.query("status");
    const excludeStatus = c.req.query("excludeStatus");

    let query = `SELECT id, title, repo_owner, repo_name, model, status, created_at, updated_at
                 FROM sessions WHERE 1=1`;
    const params: unknown[] = [];
    let paramIdx = 1;

    if (status) {
      query += ` AND status = $${paramIdx++}`;
      params.push(status);
    }
    if (excludeStatus) {
      query += ` AND status != $${paramIdx++}`;
      params.push(excludeStatus);
    }

    // Count total
    const countResult = await pool.query(
      `SELECT COUNT(*) as count FROM sessions WHERE 1=1${status ? ` AND status = $1` : ""}${excludeStatus ? ` AND status != $${status ? 2 : 1}` : ""}`,
      status && excludeStatus
        ? [status, excludeStatus]
        : status
          ? [status]
          : excludeStatus
            ? [excludeStatus]
            : []
    );
    const total = parseInt(countResult.rows[0].count, 10);

    query += ` ORDER BY created_at DESC LIMIT $${paramIdx++} OFFSET $${paramIdx++}`;
    params.push(limit, offset);

    const result = await pool.query(query, params);

    const sessions = result.rows.map((row: Record<string, unknown>) => ({
      id: row.id,
      title: row.title,
      repoOwner: row.repo_owner,
      repoName: row.repo_name,
      model: row.model,
      status: row.status,
      createdAt: Number(row.created_at),
      updatedAt: Number(row.updated_at),
    }));

    return c.json({
      sessions,
      total,
      hasMore: offset + limit < total,
    });
  });

  // Create session
  app.post("/sessions", async (c) => {
    const authErr = await requireAuth(c);
    if (authErr) return authErr;

    const body = await c.req.json();
    if (!body.repoOwner || !body.repoName) {
      return c.json({ error: "repoOwner and repoName are required" }, 400);
    }

    const repoOwner = body.repoOwner.toLowerCase();
    const repoName = body.repoName.toLowerCase();

    // Resolve repo via GitHub App
    const appConfig = getGitHubAppConfig({
      githubAppId: config.githubAppId,
      githubAppPrivateKey: config.githubAppPrivateKey,
      githubAppInstallationId: config.githubAppInstallationId,
    });

    if (!appConfig) {
      return c.json({ error: "GitHub App not configured" }, 500);
    }

    const repo = await getInstallationRepository(appConfig, repoOwner, repoName);
    if (!repo) {
      return c.json(
        { error: "Repository is not installed for the GitHub App" },
        404
      );
    }

    // Encrypt GitHub token if provided
    let githubTokenEncrypted: string | null = null;
    if (body.githubToken && config.tokenEncryptionKey) {
      githubTokenEncrypted = await encryptToken(
        body.githubToken,
        config.tokenEncryptionKey
      );
    }

    const sessionId = generateId();

    await sessionManager.createSession({
      sessionId,
      repoOwner,
      repoName,
      repoId: repo.id,
      title: body.title,
      model: body.model || "claude-haiku-4-5",
      reasoningEffort: body.reasoningEffort,
      userId: body.userId || "anonymous",
      githubLogin: body.githubLogin,
      githubName: body.githubName,
      githubEmail: body.githubEmail,
      githubTokenEncrypted,
    });

    return c.json({ sessionId, status: "created" }, 201);
  });

  // Get session
  app.get("/sessions/:id", async (c) => {
    const authErr = await requireAuth(c);
    if (authErr) return authErr;

    const sessionId = c.req.param("id");
    const session = await sessionManager.getSession(sessionId);

    if (!session) {
      return c.json({ error: "Session not found" }, 404);
    }

    return c.json(session);
  });

  // Delete session
  app.delete("/sessions/:id", async (c) => {
    const authErr = await requireAuth(c);
    if (authErr) return authErr;

    const sessionId = c.req.param("id");
    await pool.query(`DELETE FROM sessions WHERE id = $1`, [sessionId]);

    return c.json({ status: "deleted", sessionId });
  });

  // Send prompt via HTTP
  app.post("/sessions/:id/prompt", async (c) => {
    const authErr = await requireAuth(c);
    if (authErr) return authErr;

    const sessionId = c.req.param("id");
    const body = await c.req.json();

    if (!body.content) {
      return c.json({ error: "content is required" }, 400);
    }

    const messageId = generateId();
    const now = Date.now();

    await pool.query(
      `INSERT INTO messages (id, session_id, author_id, content, source, model, reasoning_effort, status, callback_context, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8, $9)`,
      [
        messageId,
        sessionId,
        body.authorId || "anonymous",
        body.content,
        body.source || "web",
        body.model || null,
        body.reasoningEffort || null,
        body.callbackContext ? JSON.stringify(body.callbackContext) : null,
        now,
      ]
    );

    return c.json({ messageId, status: "queued" }, 201);
  });

  // Stop execution
  app.post("/sessions/:id/stop", async (c) => {
    const authErr = await requireAuth(c);
    if (authErr) return authErr;

    return c.json({ status: "stop_requested" });
  });

  // Get session events
  app.get("/sessions/:id/events", async (c) => {
    const authErr = await requireAuth(c);
    if (authErr) return authErr;

    const sessionId = c.req.param("id");
    const limit = Math.min(parseInt(c.req.query("limit") || "100"), 500);
    const after = c.req.query("after");

    let query = `SELECT id, type, data, message_id, created_at FROM events
                 WHERE session_id = $1`;
    const params: unknown[] = [sessionId];
    let paramIdx = 2;

    if (after) {
      query += ` AND created_at > $${paramIdx++}`;
      params.push(parseInt(after));
    }

    query += ` ORDER BY created_at ASC LIMIT $${paramIdx++}`;
    params.push(limit + 1);

    const result = await pool.query(query, params);
    const hasMore = result.rows.length > limit;
    const events = result.rows.slice(0, limit).map((row: Record<string, unknown>) => ({
      id: row.id,
      type: row.type,
      data: row.data,
      messageId: row.message_id,
      createdAt: Number(row.created_at),
    }));

    return c.json({ events, hasMore });
  });

  // Get session artifacts
  app.get("/sessions/:id/artifacts", async (c) => {
    const authErr = await requireAuth(c);
    if (authErr) return authErr;

    const sessionId = c.req.param("id");

    const result = await pool.query(
      `SELECT id, type, url, pr_number, metadata, created_at FROM artifacts
       WHERE session_id = $1 ORDER BY created_at ASC`,
      [sessionId]
    );

    const artifacts = result.rows.map((row: Record<string, unknown>) => ({
      id: row.id,
      type: row.type,
      url: row.url,
      prNumber: row.pr_number,
      metadata: row.metadata,
      createdAt: Number(row.created_at),
    }));

    return c.json({ artifacts });
  });

  // Get session participants
  app.get("/sessions/:id/participants", async (c) => {
    const authErr = await requireAuth(c);
    if (authErr) return authErr;

    const sessionId = c.req.param("id");

    const result = await pool.query(
      `SELECT id, user_id, github_login, github_name, role, joined_at
       FROM participants WHERE session_id = $1 ORDER BY joined_at ASC`,
      [sessionId]
    );

    const participants = result.rows.map((row: Record<string, unknown>) => ({
      id: row.id,
      userId: row.user_id,
      githubLogin: row.github_login,
      githubName: row.github_name,
      role: row.role,
      joinedAt: Number(row.joined_at),
    }));

    return c.json({ participants });
  });

  // Add participant
  app.post("/sessions/:id/participants", async (c) => {
    const authErr = await requireAuth(c);
    if (authErr) return authErr;

    const sessionId = c.req.param("id");
    const body = await c.req.json();

    const participantId = generateId();
    await pool.query(
      `INSERT INTO participants (id, session_id, user_id, github_login, github_name, role, joined_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [
        participantId,
        sessionId,
        body.userId,
        body.githubLogin || null,
        body.githubName || null,
        body.role || "member",
        Date.now(),
      ]
    );

    return c.json({ participantId, status: "added" }, 201);
  });

  // Get session messages
  app.get("/sessions/:id/messages", async (c) => {
    const authErr = await requireAuth(c);
    if (authErr) return authErr;

    const sessionId = c.req.param("id");

    const result = await pool.query(
      `SELECT id, author_id, content, source, status, created_at, started_at, completed_at
       FROM messages WHERE session_id = $1 ORDER BY created_at ASC`,
      [sessionId]
    );

    const messages = result.rows.map((row: Record<string, unknown>) => ({
      id: row.id,
      authorId: row.author_id,
      content: row.content,
      source: row.source,
      status: row.status,
      createdAt: Number(row.created_at),
      startedAt: row.started_at ? Number(row.started_at) : null,
      completedAt: row.completed_at ? Number(row.completed_at) : null,
    }));

    return c.json({ messages });
  });

  // Create PR (accepts both HMAC and sandbox auth)
  app.post("/sessions/:id/pr", async (c) => {
    const hmacValid = config.internalCallbackSecret
      ? await verifyInternalToken(
          c.req.header("Authorization") || null,
          config.internalCallbackSecret
        )
      : false;

    if (!hmacValid) {
      const sessionId = c.req.param("id");
      const token = c.req.header("Authorization")?.replace("Bearer ", "");
      if (!token || !(await sessionManager.verifySandboxToken(sessionId, token))) {
        return c.json({ error: "Unauthorized" }, 401);
      }
    }

    const sessionId = c.req.param("id");
    const body = await c.req.json();

    if (!body.title?.trim() || !body.body?.trim()) {
      return c.json({ error: "title and body are required" }, 400);
    }

    const session = await sessionManager.getSession(sessionId);
    if (!session) {
      return c.json({ error: "Session not found" }, 404);
    }

    return c.json({
      status: "pr_creation_requested",
      sessionId,
      title: body.title,
    });
  });

  // Generate WS token
  app.post("/sessions/:id/ws-token", async (c) => {
    const authErr = await requireAuth(c);
    if (authErr) return authErr;

    const sessionId = c.req.param("id");
    const body = await c.req.json();

    if (!body.userId) {
      return c.json({ error: "userId is required" }, 400);
    }

    let githubTokenEncrypted: string | null = null;
    let githubRefreshTokenEncrypted: string | null = null;

    if (body.githubToken && config.tokenEncryptionKey) {
      githubTokenEncrypted = await encryptToken(
        body.githubToken,
        config.tokenEncryptionKey
      );
    }
    if (body.githubRefreshToken && config.tokenEncryptionKey) {
      githubRefreshTokenEncrypted = await encryptToken(
        body.githubRefreshToken,
        config.tokenEncryptionKey
      );
    }

    const token = await sessionManager.generateWsToken(
      sessionId,
      body.userId,
      body.githubLogin,
      body.githubName,
      body.githubEmail,
      githubTokenEncrypted,
      githubRefreshTokenEncrypted,
      body.githubTokenExpiresAt
    );

    return c.json({ token });
  });

  // Archive session
  app.post("/sessions/:id/archive", async (c) => {
    const authErr = await requireAuth(c);
    if (authErr) return authErr;

    const sessionId = c.req.param("id");
    const result = await pool.query(
      `UPDATE sessions SET status = 'archived', updated_at = $1 WHERE id = $2 RETURNING id`,
      [Date.now(), sessionId]
    );

    if (result.rowCount === 0) {
      return c.json({ error: "Session not found" }, 404);
    }

    return c.json({ status: "archived", sessionId });
  });

  // Unarchive session
  app.post("/sessions/:id/unarchive", async (c) => {
    const authErr = await requireAuth(c);
    if (authErr) return authErr;

    const sessionId = c.req.param("id");
    const result = await pool.query(
      `UPDATE sessions SET status = 'active', updated_at = $1 WHERE id = $2 RETURNING id`,
      [Date.now(), sessionId]
    );

    if (result.rowCount === 0) {
      return c.json({ error: "Session not found" }, 404);
    }

    return c.json({ status: "active", sessionId });
  });

  // ---- Repository routes ----

  // List repos
  app.get("/repos", async (c) => {
    const authErr = await requireAuth(c);
    if (authErr) return authErr;

    const cached = await redis.get<CachedReposList>(REPOS_CACHE_KEY);
    if (cached) {
      const isFresh = cached.freshUntil && Date.now() < cached.freshUntil;

      if (!isFresh) {
        refreshReposCache(pool, redis, config).catch((err) => {
          logger.error("Background repos refresh failed", {
            error: err instanceof Error ? err : String(err),
          });
        });
      }

      return c.json({
        repos: cached.repos,
        cached: true,
        cachedAt: cached.cachedAt,
      });
    }

    const appConfig = getGitHubAppConfig({
      githubAppId: config.githubAppId,
      githubAppPrivateKey: config.githubAppPrivateKey,
      githubAppInstallationId: config.githubAppInstallationId,
    });

    if (!appConfig) {
      return c.json({ error: "GitHub App not configured" }, 500);
    }

    const result = await listInstallationRepositories(appConfig);
    const enrichedRepos = await enrichReposWithMetadata(pool, result.repos);

    const cachedAt = new Date().toISOString();
    const freshUntil = Date.now() + REPOS_CACHE_FRESH_MS;

    await redis.put(
      REPOS_CACHE_KEY,
      { repos: enrichedRepos, cachedAt, freshUntil },
      REPOS_CACHE_TTL_SECONDS
    );

    return c.json({ repos: enrichedRepos, cached: false, cachedAt });
  });

  // Update repo metadata
  app.put("/repos/:owner/:name/metadata", async (c) => {
    const authErr = await requireAuth(c);
    if (authErr) return authErr;

    const owner = c.req.param("owner");
    const name = c.req.param("name");
    const body = await c.req.json();

    await pool.query(
      `INSERT INTO repo_metadata (repo_owner, repo_name, description, aliases, channel_associations, keywords, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (repo_owner, repo_name) DO UPDATE SET
         description = COALESCE($3, repo_metadata.description),
         aliases = COALESCE($4, repo_metadata.aliases),
         channel_associations = COALESCE($5, repo_metadata.channel_associations),
         keywords = COALESCE($6, repo_metadata.keywords),
         updated_at = $7`,
      [
        owner.toLowerCase(),
        name.toLowerCase(),
        body.description || null,
        body.aliases ? JSON.stringify(body.aliases) : null,
        body.channelAssociations ? JSON.stringify(body.channelAssociations) : null,
        body.keywords ? JSON.stringify(body.keywords) : null,
        Date.now(),
      ]
    );

    await redis.delete(REPOS_CACHE_KEY);

    return c.json({
      status: "updated",
      repo: `${owner.toLowerCase()}/${name.toLowerCase()}`,
      metadata: body,
    });
  });

  // Get repo metadata
  app.get("/repos/:owner/:name/metadata", async (c) => {
    const authErr = await requireAuth(c);
    if (authErr) return authErr;

    const owner = c.req.param("owner");
    const name = c.req.param("name");

    const result = await pool.query(
      `SELECT description, aliases, channel_associations, keywords
       FROM repo_metadata WHERE repo_owner = $1 AND repo_name = $2`,
      [owner.toLowerCase(), name.toLowerCase()]
    );

    const metadata =
      result.rows.length > 0
        ? {
            description: result.rows[0].description,
            aliases: result.rows[0].aliases,
            channelAssociations: result.rows[0].channel_associations,
            keywords: result.rows[0].keywords,
          }
        : null;

    return c.json({
      repo: `${owner.toLowerCase()}/${name.toLowerCase()}`,
      metadata,
    });
  });

  // Set repo secrets
  app.put("/repos/:owner/:name/secrets", async (c) => {
    const authErr = await requireAuth(c);
    if (authErr) return authErr;

    if (!config.repoSecretsEncryptionKey) {
      return c.json({ error: "REPO_SECRETS_ENCRYPTION_KEY not configured" }, 500);
    }

    const owner = c.req.param("owner");
    const name = c.req.param("name");
    const body = await c.req.json();

    if (!body?.secrets || typeof body.secrets !== "object") {
      return c.json({ error: "Request body must include secrets object" }, 400);
    }

    const appConfig = getGitHubAppConfig({
      githubAppId: config.githubAppId,
      githubAppPrivateKey: config.githubAppPrivateKey,
      githubAppInstallationId: config.githubAppInstallationId,
    });

    if (!appConfig) {
      return c.json({ error: "GitHub App not configured" }, 500);
    }

    const repo = await getInstallationRepository(appConfig, owner, name);
    if (!repo) {
      return c.json({ error: "Repository is not installed for the GitHub App" }, 404);
    }

    const repoId = repo.id;
    const repoOwner = owner.toLowerCase();
    const repoName = name.toLowerCase();

    const keys: string[] = [];
    let created = 0;
    let updated = 0;

    const nodeCrypto = await import("node:crypto");

    for (const [key, value] of Object.entries(body.secrets as Record<string, string>)) {
      const normalizedKey = key.toUpperCase().replace(/[^A-Z0-9_]/g, "_");
      const iv = nodeCrypto.randomBytes(12);
      const keyBuffer = Buffer.from(config.repoSecretsEncryptionKey, "hex");

      const cipher = nodeCrypto.createCipheriv("aes-256-gcm", keyBuffer, iv);
      let encrypted = cipher.update(value, "utf8", "hex");
      encrypted += cipher.final("hex");
      const authTag = cipher.getAuthTag().toString("hex");

      const result = await pool.query(
        `INSERT INTO repo_secrets (repo_id, repo_owner, repo_name, key, encrypted_value, iv, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
         ON CONFLICT (repo_id, key) DO UPDATE SET encrypted_value = $5, iv = $6, updated_at = $7
         RETURNING (xmax = 0) AS is_insert`,
        [repoId, repoOwner, repoName, normalizedKey, encrypted + authTag, iv.toString("hex"), Date.now()]
      );

      keys.push(normalizedKey);
      if (result.rows[0].is_insert) created++;
      else updated++;
    }

    return c.json({ status: "updated", repo: `${repoOwner}/${repoName}`, keys, created, updated });
  });

  // List repo secret keys
  app.get("/repos/:owner/:name/secrets", async (c) => {
    const authErr = await requireAuth(c);
    if (authErr) return authErr;

    if (!config.repoSecretsEncryptionKey) {
      return c.json({ error: "REPO_SECRETS_ENCRYPTION_KEY not configured" }, 500);
    }

    const owner = c.req.param("owner");
    const name = c.req.param("name");

    const appConfig = getGitHubAppConfig({
      githubAppId: config.githubAppId,
      githubAppPrivateKey: config.githubAppPrivateKey,
      githubAppInstallationId: config.githubAppInstallationId,
    });

    if (!appConfig) {
      return c.json({ error: "GitHub App not configured" }, 500);
    }

    const repo = await getInstallationRepository(appConfig, owner, name);
    if (!repo) {
      return c.json({ error: "Repository is not installed for the GitHub App" }, 404);
    }

    const result = await pool.query(
      `SELECT key, created_at, updated_at FROM repo_secrets WHERE repo_id = $1 ORDER BY key`,
      [repo.id]
    );

    return c.json({
      repo: `${owner.toLowerCase()}/${name.toLowerCase()}`,
      secrets: result.rows.map((row: Record<string, unknown>) => ({
        key: row.key,
        createdAt: Number(row.created_at),
        updatedAt: Number(row.updated_at),
      })),
    });
  });

  // Delete repo secret
  app.delete("/repos/:owner/:name/secrets/:key", async (c) => {
    const authErr = await requireAuth(c);
    if (authErr) return authErr;

    if (!config.repoSecretsEncryptionKey) {
      return c.json({ error: "REPO_SECRETS_ENCRYPTION_KEY not configured" }, 500);
    }

    const owner = c.req.param("owner");
    const name = c.req.param("name");
    const key = c.req.param("key");

    const appConfig = getGitHubAppConfig({
      githubAppId: config.githubAppId,
      githubAppPrivateKey: config.githubAppPrivateKey,
      githubAppInstallationId: config.githubAppInstallationId,
    });

    if (!appConfig) {
      return c.json({ error: "GitHub App not configured" }, 500);
    }

    const repo = await getInstallationRepository(appConfig, owner, name);
    if (!repo) {
      return c.json({ error: "Repository is not installed for the GitHub App" }, 404);
    }

    const normalizedKey = key.toUpperCase().replace(/[^A-Z0-9_]/g, "_");
    const result = await pool.query(
      `DELETE FROM repo_secrets WHERE repo_id = $1 AND key = $2`,
      [repo.id, normalizedKey]
    );

    if (result.rowCount === 0) {
      return c.json({ error: "Secret not found" }, 404);
    }

    return c.json({
      status: "deleted",
      repo: `${owner.toLowerCase()}/${name.toLowerCase()}`,
      key: normalizedKey,
    });
  });

  // 404 fallback
  app.notFound((c) => c.json({ error: "Not found" }, 404));

  return app;
}

// ---- Helper functions ----

async function enrichReposWithMetadata(
  pool: Pool,
  repos: InstallationRepository[]
): Promise<EnrichedRepository[]> {
  if (repos.length === 0) return [];

  const keys = repos.map((r) => `${r.owner.toLowerCase()}/${r.name.toLowerCase()}`);

  const result = await pool.query(
    `SELECT repo_owner, repo_name, description, aliases, channel_associations, keywords
     FROM repo_metadata
     WHERE repo_owner || '/' || repo_name = ANY($1)`,
    [keys]
  );

  const metadataMap = new Map<string, RepoMetadata>();
  for (const row of result.rows) {
    const key = `${row.repo_owner}/${row.repo_name}`;
    metadataMap.set(key, {
      description: row.description,
      aliases: row.aliases,
      channelAssociations: row.channel_associations,
      keywords: row.keywords,
    });
  }

  return repos.map((repo) => {
    const key = `${repo.owner.toLowerCase()}/${repo.name.toLowerCase()}`;
    const metadata = metadataMap.get(key);
    return metadata ? { ...repo, metadata } : repo;
  });
}

async function refreshReposCache(
  pool: Pool,
  redis: RedisCache,
  config: Config
): Promise<void> {
  const appConfig = getGitHubAppConfig({
    githubAppId: config.githubAppId,
    githubAppPrivateKey: config.githubAppPrivateKey,
    githubAppInstallationId: config.githubAppInstallationId,
  });

  if (!appConfig) return;

  const result = await listInstallationRepositories(appConfig);
  const enrichedRepos = await enrichReposWithMetadata(pool, result.repos);

  const cachedAt = new Date().toISOString();
  const freshUntil = Date.now() + REPOS_CACHE_FRESH_MS;

  await redis.put(
    REPOS_CACHE_KEY,
    { repos: enrichedRepos, cachedAt, freshUntil },
    REPOS_CACHE_TTL_SECONDS
  );
}
