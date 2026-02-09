/**
 * HTTP API server — Hono replacement for Cloudflare Workers router.
 *
 * Ports the routes from packages/control-plane/src/router.ts to Hono,
 * replacing:
 * - Cloudflare Env bindings → Config + injected dependencies
 * - D1Database → PostgreSQL pool
 * - KV Namespace → Redis cache
 * - Durable Object stubs → SessionActorRegistry
 *
 * WebSocket upgrades are handled separately by the Node.js HTTP server
 * (see index.ts) since Hono doesn't natively handle WS upgrades.
 */

import { Hono } from "hono";
import { cors } from "hono/cors";
import type pg from "pg";
import type Redis from "ioredis";
import { SessionIndexStore } from "./db/session-index.js";
import { RepoMetadataStore } from "./db/repo-metadata.js";
import { RepoSecretsStore } from "./db/repo-secrets.js";
import { ReposCacheStore } from "./db/cache.js";
import type { SessionActorRegistry } from "./session/registry.js";
import type { Config } from "./utils/config.js";
import { createLogger } from "./utils/logger.js";
import { generateId } from "./auth/crypto.js";

const log = createLogger("router");

export interface ServerDeps {
  pool: pg.Pool;
  redis: Redis;
  registry: SessionActorRegistry;
  config: Config;
}

export function createApp(deps: ServerDeps): Hono {
  const { pool, redis, registry, config } = deps;
  const app = new Hono();

  // Middleware
  app.use("*", cors({ origin: "*" }));

  // Request logging
  app.use("*", async (c, next) => {
    const traceId = c.req.header("x-trace-id") || crypto.randomUUID();
    const requestId = crypto.randomUUID().slice(0, 8);
    c.set("trace_id", traceId);
    c.set("request_id", requestId);

    const start = Date.now();
    await next();

    log.info("http.request", {
      event: "http.request",
      trace_id: traceId,
      request_id: requestId,
      http_method: c.req.method,
      http_path: c.req.path,
      http_status: c.res.status,
      duration_ms: Date.now() - start,
    });
  });

  // ── Health ──

  app.get("/health", (c) => {
    return c.json({
      status: "ok",
      version: "k8s-0.1.0",
      activeActors: registry.size,
    });
  });

  // ── Sessions ──

  app.get("/sessions", async (c) => {
    const store = new SessionIndexStore(pool);
    const status = c.req.query("status");
    const excludeStatus = c.req.query("exclude_status");
    const repoOwner = c.req.query("repo_owner");
    const repoName = c.req.query("repo_name");
    const limit = parseInt(c.req.query("limit") || "50");
    const offset = parseInt(c.req.query("offset") || "0");

    const result = await store.list({ status, excludeStatus, repoOwner, repoName, limit, offset });
    return c.json(result);
  });

  app.post("/sessions", async (c) => {
    const body = await c.req.json();
    const sessionId = generateId();

    // Create in session index
    const store = new SessionIndexStore(pool);
    const now = Date.now();
    await store.create({
      id: sessionId,
      title: body.title ?? null,
      repoOwner: body.repoOwner,
      repoName: body.repoName,
      model: body.model || "claude-haiku-4-5",
      status: "created",
      createdAt: now,
      updatedAt: now,
    });

    // Initialize actor
    const actor = registry.getOrCreate(sessionId);
    const result = await actor.handleInit({
      sessionName: body.sessionName || sessionId,
      repoOwner: body.repoOwner,
      repoName: body.repoName,
      repoId: body.repoId,
      title: body.title,
      model: body.model,
      reasoningEffort: body.reasoningEffort,
      userId: body.userId || "anonymous",
      githubLogin: body.githubLogin,
      githubName: body.githubName,
      githubEmail: body.githubEmail,
      githubToken: body.githubToken,
      githubTokenEncrypted: body.githubTokenEncrypted,
    });

    return c.json(result, 201);
  });

  app.get("/sessions/:id", async (c) => {
    const sessionId = c.req.param("id");
    const actor = registry.getOrCreate(sessionId);
    const state = await actor.handleGetState();

    if (!state) return c.json({ error: "Session not found" }, 404);
    return c.json(state);
  });

  app.delete("/sessions/:id", async (c) => {
    const sessionId = c.req.param("id");
    const store = new SessionIndexStore(pool);
    const deleted = await store.delete(sessionId);

    if (!deleted) return c.json({ error: "Session not found" }, 404);

    registry.evict(sessionId);
    return c.json({ status: "deleted" });
  });

  // ── Session Operations ──

  app.post("/sessions/:id/prompt", async (c) => {
    const sessionId = c.req.param("id");
    const body = await c.req.json();
    const actor = registry.getOrCreate(sessionId);

    const result = await actor.handleEnqueuePrompt(body);
    return c.json(result);
  });

  app.post("/sessions/:id/stop", async (c) => {
    const sessionId = c.req.param("id");
    const actor = registry.getOrCreate(sessionId);

    await actor.handleStop();
    return c.json({ status: "stopping" });
  });

  app.get("/sessions/:id/events", async (c) => {
    const sessionId = c.req.param("id");
    const actor = registry.getOrCreate(sessionId);

    const result = await actor.handleListEvents({
      cursor: c.req.query("cursor"),
      limit: c.req.query("limit") ? parseInt(c.req.query("limit")!) : undefined,
      type: c.req.query("type"),
      messageId: c.req.query("message_id"),
    });

    return c.json(result);
  });

  app.post("/sessions/:id/ws-token", async (c) => {
    const sessionId = c.req.param("id");
    const body = await c.req.json();
    const actor = registry.getOrCreate(sessionId);

    const result = await actor.handleGenerateWsToken(body);
    return c.json(result);
  });

  app.post("/sessions/:id/archive", async (c) => {
    const sessionId = c.req.param("id");
    const body = await c.req.json();
    const actor = registry.getOrCreate(sessionId);

    const result = await actor.handleArchive(body.userId);
    if ("error" in result) return c.json(result, 403);
    return c.json(result);
  });

  // ── Repos ──

  app.get("/repos", async (c) => {
    const cache = new ReposCacheStore(redis);
    const cached = await cache.get();

    if (cached) {
      if (!cache.isFresh(cached)) {
        // Stale — serve immediately but refresh in background
        // (In CF this was ctx.executionCtx.waitUntil; here we fire-and-forget)
        refreshReposInBackground(pool, redis, config).catch((e) => {
          log.error("Background repos refresh failed", { error: e });
        });
      }
      return c.json({ repos: cached.repos, cached: true });
    }

    // No cache — fetch synchronously
    // TODO: Implement GitHub App repo listing
    return c.json({ repos: [], cached: false });
  });

  app.get("/repos/:owner/:name/metadata", async (c) => {
    const store = new RepoMetadataStore(pool);
    const metadata = await store.get(c.req.param("owner"), c.req.param("name"));
    return c.json({ metadata: metadata || {} });
  });

  app.put("/repos/:owner/:name/metadata", async (c) => {
    const store = new RepoMetadataStore(pool);
    const cache = new ReposCacheStore(redis);
    const body = await c.req.json();

    await store.upsert(c.req.param("owner"), c.req.param("name"), body);
    await cache.invalidate();

    return c.json({ status: "updated" });
  });

  // ── Repo Secrets ──

  app.get("/repos/:owner/:name/secrets", async (c) => {
    if (!config.repoSecretsEncryptionKey) {
      return c.json({ error: "Secrets not configured" }, 503);
    }

    const store = new RepoSecretsStore(pool, config.repoSecretsEncryptionKey);
    const repoId = parseInt(c.req.query("repo_id") || "0");
    if (!repoId) return c.json({ error: "repo_id required" }, 400);

    const keys = await store.listSecretKeys(repoId);
    return c.json({ secrets: keys });
  });

  app.put("/repos/:owner/:name/secrets", async (c) => {
    if (!config.repoSecretsEncryptionKey) {
      return c.json({ error: "Secrets not configured" }, 503);
    }

    const store = new RepoSecretsStore(pool, config.repoSecretsEncryptionKey);
    const body = await c.req.json();
    const repoId = body.repoId;
    if (!repoId) return c.json({ error: "repoId required" }, 400);

    const result = await store.setSecrets(
      repoId,
      c.req.param("owner"),
      c.req.param("name"),
      body.secrets
    );
    return c.json(result);
  });

  app.delete("/repos/:owner/:name/secrets/:key", async (c) => {
    if (!config.repoSecretsEncryptionKey) {
      return c.json({ error: "Secrets not configured" }, 503);
    }

    const store = new RepoSecretsStore(pool, config.repoSecretsEncryptionKey);
    const repoId = parseInt(c.req.query("repo_id") || "0");
    if (!repoId) return c.json({ error: "repo_id required" }, 400);

    const deleted = await store.deleteSecret(repoId, c.req.param("key"));
    if (!deleted) return c.json({ error: "Secret not found" }, 404);
    return c.json({ status: "deleted" });
  });

  return app;
}

async function refreshReposInBackground(
  _pool: pg.Pool,
  redis: Redis,
  _config: Config
): Promise<void> {
  // TODO: Implement GitHub App repo listing + metadata enrichment
  const cache = new ReposCacheStore(redis);
  await cache.set([]);
}
