/**
 * Open-Inspect Control Plane Server
 *
 * Replaces the Cloudflare Workers entry point (index.ts).
 * Runs as a standalone Node.js HTTP server with:
 * - Hono for routing (lightweight, works like Workers fetch API)
 * - Rivet actors for per-session state management
 * - PostgreSQL for persistent storage
 * - Redis for caching
 * - Standard WebSocket (ws) for real-time connections
 *
 * All Cloudflare-specific bindings (Durable Objects, D1, KV, ExecutionContext)
 * are replaced with their K8s-native equivalents.
 */

import { Hono } from "hono";
import { cors } from "hono/cors";
import { serve } from "@hono/node-server";
import { createNodeWebSocket } from "@hono/node-ws";
import { runMigrations } from "./db/migrate";
import { createLogger } from "./logger";
import { verifyInternalToken } from "./auth/internal";
import { generateId, encryptToken } from "./auth/crypto";
import {
  getGitHubAppConfig,
  getInstallationRepository,
  listInstallationRepositories,
} from "./auth/github-app";
import { PgSessionIndexStore } from "./db/pg-session-index";
import { PgRepoMetadataStore } from "./db/pg-repo-metadata";
import { PgRepoSecretsStore, RepoSecretsValidationError } from "./db/pg-repo-secrets";
import { PgSessionRepository } from "./session/pg-repository";
import { SessionActorManager } from "./actors/session-actor-manager";
import { RedisCache } from "./cache/redis";
import type {
  EnrichedRepository,
  InstallationRepository,
  RepoMetadata,
} from "@open-inspect/shared";

const logger = createLogger("server");

// Environment configuration (replaces Cloudflare Env bindings)
export interface ServerConfig {
  port: number;
  databaseUrl: string;
  redisUrl: string;
  sandboxApiUrl: string;       // URL of the K8s sandbox-api service
  apiSecret: string;           // Shared HMAC secret for internal auth
  tokenEncryptionKey: string;  // AES-256 key for token encryption
  repoSecretsKey?: string;     // AES-256 key for repo secrets encryption
  githubAppId?: string;
  githubAppPrivateKey?: string;
  githubAppInstallationId?: string;
  githubClientId?: string;
  githubClientSecret?: string;
  workerUrl?: string;          // Public URL of this service
  webAppUrl?: string;          // Public URL of the web app
  logLevel?: string;
}

function loadConfig(): ServerConfig {
  return {
    port: parseInt(process.env.PORT || "8787"),
    databaseUrl: process.env.DATABASE_URL || "postgresql://postgres:postgres@localhost:5432/open_inspect",
    redisUrl: process.env.REDIS_URL || "redis://localhost:6379",
    sandboxApiUrl: process.env.SANDBOX_API_URL || "http://sandbox-api:8080",
    apiSecret: process.env.API_SECRET || "",
    tokenEncryptionKey: process.env.TOKEN_ENCRYPTION_KEY || "",
    repoSecretsKey: process.env.REPO_SECRETS_ENCRYPTION_KEY,
    githubAppId: process.env.GITHUB_APP_ID,
    githubAppPrivateKey: process.env.GITHUB_APP_PRIVATE_KEY,
    githubAppInstallationId: process.env.GITHUB_APP_INSTALLATION_ID,
    githubClientId: process.env.GITHUB_CLIENT_ID,
    githubClientSecret: process.env.GITHUB_CLIENT_SECRET,
    workerUrl: process.env.WORKER_URL,
    webAppUrl: process.env.WEB_APP_URL,
    logLevel: process.env.LOG_LEVEL,
  };
}

// Cache constants
const REPOS_CACHE_KEY = "repos:list";
const REPOS_CACHE_TTL_SECONDS = 300; // 5 minutes

export async function createServer() {
  const config = loadConfig();
  const app = new Hono();
  const { injectWebSocket, upgradeWebSocket } = createNodeWebSocket({ app });

  // Run database migrations on startup
  await runMigrations();

  // Initialize stores
  const sessionIndex = new PgSessionIndexStore();
  const repoMetadata = new PgRepoMetadataStore();
  const repoSecrets = config.repoSecretsKey
    ? new PgRepoSecretsStore(config.repoSecretsKey)
    : null;
  const sessionRepo = new PgSessionRepository();
  const cache = new RedisCache(config.redisUrl);
  const actorManager = new SessionActorManager(sessionRepo, config);

  // CORS
  app.use("*", cors({
    origin: "*",
    allowMethods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowHeaders: ["Content-Type", "Authorization"],
    maxAge: 86400,
  }));

  // Request context middleware
  app.use("*", async (c, next) => {
    const traceId = c.req.header("x-trace-id") || crypto.randomUUID();
    const requestId = crypto.randomUUID().slice(0, 8);
    c.set("traceId", traceId);
    c.set("requestId", requestId);
    c.header("x-trace-id", traceId);
    c.header("x-request-id", requestId);
    await next();
  });

  // Auth middleware (skip for health and OPTIONS)
  app.use("*", async (c, next) => {
    const path = c.req.path;
    const method = c.req.method;

    if (method === "OPTIONS" || path === "/health") {
      return next();
    }

    const isValid = await verifyInternalToken(
      c.req.header("Authorization"),
      config.apiSecret
    );

    if (!isValid) {
      return c.json({ error: "Unauthorized" }, 401);
    }

    await next();
  });

  // ===== Health =====
  app.get("/health", (c) =>
    c.json({ status: "healthy", service: "open-inspect-control-plane" })
  );

  // ===== Sessions =====

  app.get("/sessions", async (c) => {
    const limit = Math.min(parseInt(c.req.query("limit") || "50"), 100);
    const offset = parseInt(c.req.query("offset") || "0");
    const status = c.req.query("status") || undefined;
    const excludeStatus = c.req.query("excludeStatus") || undefined;

    const result = await sessionIndex.list({ status, excludeStatus, limit, offset });
    return c.json({
      sessions: result.sessions,
      total: result.total,
      hasMore: offset + limit < result.total,
    });
  });

  app.post("/sessions", async (c) => {
    const body = await c.req.json();

    if (!body.repoOwner || !body.repoName) {
      return c.json({ error: "repoOwner and repoName are required" }, 400);
    }

    const repoOwner = body.repoOwner.toLowerCase();
    const repoName = body.repoName.toLowerCase();

    // Resolve repo via GitHub App
    const appConfig = getGitHubAppConfig({
      GITHUB_APP_ID: config.githubAppId,
      GITHUB_APP_PRIVATE_KEY: config.githubAppPrivateKey,
      GITHUB_APP_INSTALLATION_ID: config.githubAppInstallationId,
    });
    if (!appConfig) {
      return c.json({ error: "GitHub App not configured" }, 500);
    }

    const repo = await getInstallationRepository(appConfig, repoOwner, repoName);
    if (!repo) {
      return c.json({ error: "Repository is not installed for the GitHub App" }, 404);
    }

    const sessionId = generateId();
    const userId = body.userId || "anonymous";
    const now = Date.now();

    // Encrypt GitHub token if provided
    let githubTokenEncrypted: string | null = null;
    if (body.githubToken && config.tokenEncryptionKey) {
      githubTokenEncrypted = await encryptToken(body.githubToken, config.tokenEncryptionKey);
    }

    // Create session in database
    await sessionRepo.upsertSession({
      id: sessionId,
      sessionName: sessionId,
      title: body.title,
      repoOwner,
      repoName,
      repoId: repo.id,
      model: body.model || "claude-haiku-4-5",
      reasoningEffort: body.reasoningEffort,
      ownerUserId: userId,
    });

    // Create session in index
    await sessionIndex.create({
      id: sessionId,
      owner_user_id: userId,
      title: body.title || null,
      repo_owner: repoOwner,
      repo_name: repoName,
      repo_id: repo.id,
      status: "created",
      created_at: now,
      updated_at: now,
    });

    // Create owner participant
    const participantId = generateId();
    await sessionRepo.createParticipant({
      id: participantId,
      sessionId,
      userId,
      githubUserId: body.githubUserId,
      githubLogin: body.githubLogin,
      githubEmail: body.githubEmail,
      githubName: body.githubName,
      role: "owner",
      githubAccessTokenEncrypted: githubTokenEncrypted ?? undefined,
    });

    // Initialize sandbox record
    await sessionRepo.createSandbox({
      id: generateId(),
      sessionId,
      status: "pending",
    });

    // Initialize session actor
    actorManager.initSession(sessionId);

    return c.json({ sessionId, status: "created" }, 201);
  });

  app.get("/sessions/:id", async (c) => {
    const sessionId = c.req.param("id");
    const session = await sessionRepo.getSession(sessionId);
    if (!session) return c.json({ error: "Session not found" }, 404);
    return c.json(session);
  });

  app.delete("/sessions/:id", async (c) => {
    const sessionId = c.req.param("id");
    await sessionIndex.delete(sessionId);
    actorManager.destroySession(sessionId);
    return c.json({ status: "deleted", sessionId });
  });

  app.post("/sessions/:id/prompt", async (c) => {
    const sessionId = c.req.param("id");
    const body = await c.req.json();

    if (!body.content) return c.json({ error: "content is required" }, 400);

    const messageId = generateId();
    await sessionRepo.createMessage({
      id: messageId,
      sessionId,
      authorId: body.authorId || "anonymous",
      content: body.content,
      source: body.source || "web",
      model: body.model,
      reasoningEffort: body.reasoningEffort,
      attachments: body.attachments ? JSON.stringify(body.attachments) : undefined,
      callbackContext: body.callbackContext ? JSON.stringify(body.callbackContext) : undefined,
    });

    // Notify the session actor to process the message
    await actorManager.enqueuePrompt(sessionId, messageId);

    const pendingCount = await sessionRepo.getPendingOrProcessingCount(sessionId);
    return c.json({ messageId, position: pendingCount });
  });

  app.post("/sessions/:id/stop", async (c) => {
    const sessionId = c.req.param("id");
    await actorManager.stopExecution(sessionId);
    return c.json({ status: "stopped" });
  });

  app.get("/sessions/:id/events", async (c) => {
    const sessionId = c.req.param("id");
    const limit = parseInt(c.req.query("limit") || "100");
    const events = await sessionRepo.listEvents({ sessionId, limit });
    return c.json({ events });
  });

  app.get("/sessions/:id/artifacts", async (c) => {
    const sessionId = c.req.param("id");
    const artifacts = await sessionRepo.listArtifacts(sessionId);
    return c.json({ artifacts });
  });

  app.get("/sessions/:id/participants", async (c) => {
    const sessionId = c.req.param("id");
    const participants = await sessionRepo.listParticipants(sessionId);
    return c.json({ participants });
  });

  app.post("/sessions/:id/participants", async (c) => {
    const sessionId = c.req.param("id");
    const body = await c.req.json();
    const participantId = generateId();

    let githubTokenEncrypted: string | undefined;
    if (body.githubToken && config.tokenEncryptionKey) {
      githubTokenEncrypted = await encryptToken(body.githubToken, config.tokenEncryptionKey);
    }

    await sessionRepo.createParticipant({
      id: participantId,
      sessionId,
      userId: body.userId,
      githubUserId: body.githubUserId,
      githubLogin: body.githubLogin,
      githubEmail: body.githubEmail,
      githubName: body.githubName,
      role: body.role || "member",
      githubAccessTokenEncrypted: githubTokenEncrypted,
    });

    return c.json({ participantId }, 201);
  });

  app.get("/sessions/:id/messages", async (c) => {
    const sessionId = c.req.param("id");
    const limit = parseInt(c.req.query("limit") || "50");
    const offset = parseInt(c.req.query("offset") || "0");
    const messages = await sessionRepo.listMessages(sessionId, limit, offset);
    return c.json({ messages });
  });

  app.post("/sessions/:id/ws-token", async (c) => {
    const sessionId = c.req.param("id");
    const body = await c.req.json();

    if (!body.userId) return c.json({ error: "userId is required" }, 400);

    const token = await actorManager.generateWsToken(sessionId, body);
    return c.json({ token });
  });

  app.post("/sessions/:id/pr", async (c) => {
    const sessionId = c.req.param("id");
    const body = await c.req.json();
    const result = await actorManager.createPR(sessionId, body);
    return c.json(result);
  });

  app.post("/sessions/:id/archive", async (c) => {
    const sessionId = c.req.param("id");
    await sessionRepo.updateSessionStatus(sessionId, "archived");
    await sessionIndex.updateStatus(sessionId, "archived");
    actorManager.destroySession(sessionId);
    return c.json({ status: "archived" });
  });

  app.post("/sessions/:id/unarchive", async (c) => {
    const sessionId = c.req.param("id");
    await sessionRepo.updateSessionStatus(sessionId, "active");
    await sessionIndex.updateStatus(sessionId, "active");
    return c.json({ status: "active" });
  });

  // ===== Repositories =====

  app.get("/repos", async (c) => {
    // Check Redis cache first
    const cached = await cache.get<{ repos: EnrichedRepository[]; cachedAt: string }>(REPOS_CACHE_KEY);
    if (cached) {
      return c.json({ repos: cached.repos, cached: true, cachedAt: cached.cachedAt });
    }

    const appConfig = getGitHubAppConfig({
      GITHUB_APP_ID: config.githubAppId,
      GITHUB_APP_PRIVATE_KEY: config.githubAppPrivateKey,
      GITHUB_APP_INSTALLATION_ID: config.githubAppInstallationId,
    });

    if (!appConfig) return c.json({ error: "GitHub App not configured" }, 500);

    const result = await listInstallationRepositories(appConfig);
    const repos = result.repos;

    const metadataMap = await repoMetadata.getBatch(
      repos.map((r: InstallationRepository) => ({ owner: r.owner, name: r.name }))
    );

    const enrichedRepos: EnrichedRepository[] = repos.map((repo: InstallationRepository) => {
      const key = `${repo.owner.toLowerCase()}/${repo.name.toLowerCase()}`;
      const metadata = metadataMap.get(key);
      return metadata ? { ...repo, metadata } : repo;
    });

    const cachedAt = new Date().toISOString();
    await cache.set(REPOS_CACHE_KEY, { repos: enrichedRepos, cachedAt }, REPOS_CACHE_TTL_SECONDS);

    return c.json({ repos: enrichedRepos, cached: false, cachedAt });
  });

  app.get("/repos/:owner/:name/metadata", async (c) => {
    const owner = c.req.param("owner");
    const name = c.req.param("name");
    const metadata = await repoMetadata.get(owner, name);
    return c.json({ repo: `${owner}/${name}`, metadata: metadata ?? null });
  });

  app.put("/repos/:owner/:name/metadata", async (c) => {
    const owner = c.req.param("owner");
    const name = c.req.param("name");
    const body = await c.req.json();
    await repoMetadata.upsert(owner, name, body);
    await cache.del(REPOS_CACHE_KEY);
    return c.json({ status: "updated", repo: `${owner}/${name}` });
  });

  // ===== Repo Secrets =====

  app.get("/repos/:owner/:name/secrets", async (c) => {
    if (!repoSecrets) return c.json({ error: "Secrets not configured" }, 503);
    const owner = c.req.param("owner");
    const name = c.req.param("name");
    const secrets = await repoSecrets.listSecretKeys(owner, name);
    return c.json({ repo: `${owner}/${name}`, secrets });
  });

  app.put("/repos/:owner/:name/secrets", async (c) => {
    if (!repoSecrets) return c.json({ error: "Secrets not configured" }, 503);
    const owner = c.req.param("owner");
    const name = c.req.param("name");
    const body = await c.req.json();

    if (!body?.secrets) return c.json({ error: "secrets object required" }, 400);

    try {
      await repoSecrets.setSecrets("", owner, name, body.secrets);
      return c.json({ status: "updated", repo: `${owner}/${name}` });
    } catch (e) {
      if (e instanceof RepoSecretsValidationError) {
        return c.json({ error: e.message }, 400);
      }
      throw e;
    }
  });

  app.delete("/repos/:owner/:name/secrets/:key", async (c) => {
    if (!repoSecrets) return c.json({ error: "Secrets not configured" }, 503);
    const owner = c.req.param("owner");
    const name = c.req.param("name");
    const key = c.req.param("key");
    await repoSecrets.deleteSecret(owner, name, key);
    return c.json({ status: "deleted", repo: `${owner}/${name}`, key });
  });

  // ===== WebSocket =====

  app.get(
    "/sessions/:id/ws",
    upgradeWebSocket((c) => {
      const sessionId = c.req.param("id");
      return {
        onOpen(event, ws) {
          actorManager.handleWsConnect(sessionId, ws);
        },
        onMessage(event, ws) {
          actorManager.handleWsMessage(sessionId, ws, event.data.toString());
        },
        onClose(event, ws) {
          actorManager.handleWsDisconnect(sessionId, ws);
        },
      };
    })
  );

  // Start server
  const server = serve({ fetch: app.fetch, port: config.port }, (info) => {
    logger.info("Server started", { port: info.port });
  });

  injectWebSocket(server);

  return server;
}

// Run
createServer().catch((e) => {
  console.error("Failed to start server:", e);
  process.exit(1);
});
