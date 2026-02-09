/**
 * Session Manager for Open-Inspect Control Plane.
 *
 * Replaces Cloudflare Durable Objects with in-memory session state
 * backed by PostgreSQL persistence. Each session has a SessionInstance
 * that holds WebSocket connections and sandbox state.
 */

import type { Pool, PoolClient } from "pg";
import type WebSocket from "ws";
import crypto from "node:crypto";
import { generateId, encryptToken, decryptToken } from "../auth/crypto";
import {
  getGitHubAppConfig,
  generateInstallationToken,
} from "../auth/github-app";
import { createLogger } from "../logger";
import type { Config } from "../config";
import type { RivetSandboxProvider } from "../sandbox/providers/rivet-provider";
import type { RedisCache } from "../db/redis";
import type {
  ClientMessage,
  ServerMessage,
  SessionState,
  SandboxEvent,
  ParticipantPresence,
} from "../types";

const logger = createLogger("session-manager");

const AUTH_TIMEOUT_MS = 30_000;
const HEARTBEAT_TIMEOUT_MS = 90_000;
const WS_TOKEN_EXPIRY_MS = 5 * 60 * 1000; // 5 minutes
const REPLAY_PAGE_SIZE = 200;

interface ClientInfo {
  ws: WebSocket;
  participantId: string;
  userId: string;
  name: string;
  avatar?: string;
  status: "active" | "idle" | "away";
  lastSeen: number;
  clientId: string;
}

interface SessionInstance {
  sessionId: string;
  clients: Map<string, ClientInfo>; // clientId -> info
  sandboxWs: WebSocket | null;
  sandboxActorId: string | null;
  processingMessageId: string | null;
  heartbeatTimer: ReturnType<typeof setTimeout> | null;
  inactivityTimer: ReturnType<typeof setTimeout> | null;
  lastHeartbeat: number;
}

export class SessionManager {
  private sessions = new Map<string, SessionInstance>();
  private pool: Pool;
  private config: Config;
  private sandboxProvider: RivetSandboxProvider;
  private redis: RedisCache;

  constructor(
    pool: Pool,
    config: Config,
    sandboxProvider: RivetSandboxProvider,
    redis: RedisCache
  ) {
    this.pool = pool;
    this.config = config;
    this.sandboxProvider = sandboxProvider;
    this.redis = redis;
  }

  private getOrCreateInstance(sessionId: string): SessionInstance {
    let instance = this.sessions.get(sessionId);
    if (!instance) {
      instance = {
        sessionId,
        clients: new Map(),
        sandboxWs: null,
        sandboxActorId: null,
        processingMessageId: null,
        heartbeatTimer: null,
        inactivityTimer: null,
        lastHeartbeat: 0,
      };
      this.sessions.set(sessionId, instance);
    }
    return instance;
  }

  /**
   * Handle a new client WebSocket connection.
   */
  handleWebSocketConnection(sessionId: string, ws: WebSocket): void {
    const instance = this.getOrCreateInstance(sessionId);

    // Set auth timeout - client must send subscribe within 30s
    const authTimeout = setTimeout(() => {
      logger.warn("Client auth timeout", { session_id: sessionId });
      ws.close(4001, "Authentication timeout");
    }, AUTH_TIMEOUT_MS);

    ws.on("message", async (data: Buffer | string) => {
      try {
        const msg = JSON.parse(data.toString()) as ClientMessage;
        await this.handleClientMessage(instance, ws, msg, authTimeout);
      } catch (err) {
        logger.error("Failed to handle client message", {
          session_id: sessionId,
          error: err instanceof Error ? err : String(err),
        });
      }
    });

    ws.on("close", () => {
      clearTimeout(authTimeout);
      this.handleClientDisconnect(instance, ws);
    });

    ws.on("error", (err) => {
      logger.error("Client WebSocket error", {
        session_id: sessionId,
        error: err,
      });
    });
  }

  /**
   * Handle a sandbox WebSocket connection (from Rivet actor).
   */
  handleSandboxConnection(sessionId: string, ws: WebSocket): void {
    const instance = this.getOrCreateInstance(sessionId);
    instance.sandboxWs = ws;
    instance.lastHeartbeat = Date.now();

    this.startHeartbeatMonitor(instance);

    ws.on("message", async (data: Buffer | string) => {
      try {
        const event = JSON.parse(data.toString()) as SandboxEvent;
        await this.handleSandboxEvent(instance, event);
      } catch (err) {
        logger.error("Failed to handle sandbox event", {
          session_id: sessionId,
          error: err instanceof Error ? err : String(err),
        });
      }
    });

    ws.on("close", () => {
      instance.sandboxWs = null;
      this.clearHeartbeatMonitor(instance);
      this.broadcast(instance, { type: "sandbox_status", status: "stopped" });
      this.updateSessionField(sessionId, "sandbox_status", "stopped");
    });

    ws.on("error", (err) => {
      logger.error("Sandbox WebSocket error", {
        session_id: sessionId,
        error: err,
      });
    });

    // Notify clients
    this.broadcast(instance, { type: "sandbox_ready" });
    this.updateSessionField(sessionId, "sandbox_status", "ready");
  }

  private async handleClientMessage(
    instance: SessionInstance,
    ws: WebSocket,
    msg: ClientMessage,
    authTimeout: ReturnType<typeof setTimeout>
  ): Promise<void> {
    switch (msg.type) {
      case "ping":
        this.send(ws, { type: "pong", timestamp: Date.now() });
        break;

      case "subscribe":
        clearTimeout(authTimeout);
        await this.handleSubscribe(instance, ws, msg.token, msg.clientId);
        break;

      case "prompt":
        await this.handlePrompt(instance, ws, msg);
        break;

      case "stop":
        await this.handleStop(instance);
        break;

      case "typing":
        // Broadcast typing indicator to other clients
        break;

      case "fetch_history":
        await this.handleFetchHistory(instance, ws, msg.cursor, msg.limit);
        break;
    }
  }

  private async handleSubscribe(
    instance: SessionInstance,
    ws: WebSocket,
    token: string,
    clientId: string
  ): Promise<void> {
    // Validate the WS token
    const tokenHash = crypto
      .createHash("sha256")
      .update(token)
      .digest("hex");

    const tokenRow = await this.pool.query(
      `SELECT session_id, user_id, github_login, github_name, github_email,
              github_token_encrypted, github_refresh_token_encrypted, github_token_expires_at
       FROM ws_tokens WHERE token_hash = $1 AND expires_at > $2`,
      [tokenHash, Date.now()]
    );

    if (tokenRow.rows.length === 0) {
      ws.close(4001, "Invalid or expired token");
      return;
    }

    const tokenData = tokenRow.rows[0];
    if (tokenData.session_id !== instance.sessionId) {
      ws.close(4001, "Token session mismatch");
      return;
    }

    // Ensure participant exists
    const participantId = await this.ensureParticipant(
      instance.sessionId,
      tokenData.user_id,
      tokenData.github_login,
      tokenData.github_name,
      tokenData.github_email,
      tokenData.github_token_encrypted,
      tokenData.github_refresh_token_encrypted,
      tokenData.github_token_expires_at
    );

    // Register client
    const clientInfo: ClientInfo = {
      ws,
      participantId,
      userId: tokenData.user_id,
      name: tokenData.github_name || tokenData.github_login || tokenData.user_id,
      avatar: undefined,
      status: "active",
      lastSeen: Date.now(),
      clientId,
    };
    instance.clients.set(clientId, clientInfo);

    // Get session state
    const state = await this.getSessionState(instance.sessionId);

    this.send(ws, {
      type: "subscribed",
      sessionId: instance.sessionId,
      state,
      participantId,
      participant: {
        participantId,
        name: clientInfo.name,
        avatar: clientInfo.avatar,
      },
    });

    // Send event replay
    await this.replayEvents(instance, ws);

    // Broadcast presence
    this.broadcastPresence(instance);

    // Reset inactivity timer
    this.resetInactivityTimer(instance);
  }

  private async handlePrompt(
    instance: SessionInstance,
    ws: WebSocket,
    msg: { type: "prompt"; content: string; model?: string; reasoningEffort?: string }
  ): Promise<void> {
    // Find the client info for this WebSocket
    let authorClient: ClientInfo | undefined;
    for (const client of instance.clients.values()) {
      if (client.ws === ws) {
        authorClient = client;
        break;
      }
    }

    if (!authorClient) {
      this.send(ws, { type: "error", code: "not_subscribed", message: "Not subscribed" });
      return;
    }

    const messageId = generateId();
    const now = Date.now();

    // Store message in DB
    await this.pool.query(
      `INSERT INTO messages (id, session_id, author_id, content, source, model, reasoning_effort, status, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8)`,
      [
        messageId,
        instance.sessionId,
        authorClient.userId,
        msg.content,
        "web",
        msg.model || null,
        msg.reasoningEffort || null,
        now,
      ]
    );

    // Store user_message event
    const userEvent: SandboxEvent = {
      type: "user_message",
      content: msg.content,
      messageId,
      timestamp: now,
      author: {
        participantId: authorClient.participantId,
        name: authorClient.name,
        avatar: authorClient.avatar,
      },
    };
    await this.storeEvent(instance.sessionId, userEvent, messageId);
    this.broadcast(instance, { type: "sandbox_event", event: userEvent });

    // Update model if specified
    if (msg.model) {
      await this.updateSessionField(instance.sessionId, "model", msg.model);
    }

    // Ensure sandbox is running
    if (!instance.sandboxWs) {
      await this.spawnSandbox(instance);
    }

    // Send prompt to sandbox
    if (instance.sandboxWs && instance.sandboxWs.readyState === 1) {
      instance.processingMessageId = messageId;
      this.broadcast(instance, { type: "processing_status", isProcessing: true });

      instance.sandboxWs.send(
        JSON.stringify({
          type: "prompt",
          messageId,
          content: msg.content,
          model: msg.model,
          reasoningEffort: msg.reasoningEffort,
        })
      );

      await this.pool.query(
        `UPDATE messages SET status = 'processing', started_at = $1 WHERE id = $2`,
        [Date.now(), messageId]
      );
    } else {
      this.send(ws, {
        type: "error",
        code: "sandbox_unavailable",
        message: "Sandbox is not connected",
      });
    }

    // Notify queue position
    this.send(ws, { type: "prompt_queued", messageId, position: 0 });
  }

  private async handleStop(instance: SessionInstance): Promise<void> {
    if (instance.sandboxWs && instance.sandboxWs.readyState === 1) {
      instance.sandboxWs.send(JSON.stringify({ type: "stop" }));
    }
  }

  private async handleFetchHistory(
    instance: SessionInstance,
    ws: WebSocket,
    cursor: { timestamp: number; id: string },
    limit?: number
  ): Promise<void> {
    const pageSize = Math.min(limit || REPLAY_PAGE_SIZE, REPLAY_PAGE_SIZE);

    const result = await this.pool.query(
      `SELECT id, type, data, message_id, created_at FROM events
       WHERE session_id = $1 AND (created_at < $2 OR (created_at = $2 AND id < $3))
       ORDER BY created_at DESC, id DESC
       LIMIT $4`,
      [instance.sessionId, cursor.timestamp, cursor.id, pageSize + 1]
    );

    const hasMore = result.rows.length > pageSize;
    const items = result.rows.slice(0, pageSize).reverse();

    const events = items.map((row: { id: string; type: string; data: Record<string, unknown>; message_id: string | null; created_at: number }) => ({
      ...row.data,
      type: row.type,
      messageId: row.message_id,
      timestamp: Number(row.created_at),
    }));

    const newCursor =
      items.length > 0
        ? { timestamp: Number(items[0].created_at), id: items[0].id as string }
        : null;

    this.send(ws, {
      type: "history_page",
      items: events as SandboxEvent[],
      hasMore,
      cursor: newCursor,
    });
  }

  private async handleSandboxEvent(
    instance: SessionInstance,
    event: SandboxEvent
  ): Promise<void> {
    // Update heartbeat on any sandbox event
    instance.lastHeartbeat = Date.now();

    if (event.type === "heartbeat") {
      // Just update the heartbeat timestamp, don't store or broadcast
      return;
    }

    // Store event in DB
    const messageId = "messageId" in event ? (event as { messageId?: string }).messageId : undefined;
    await this.storeEvent(instance.sessionId, event, messageId);

    // Broadcast to all clients
    this.broadcast(instance, { type: "sandbox_event", event });

    // Handle completion
    if (event.type === "execution_complete") {
      instance.processingMessageId = null;
      this.broadcast(instance, { type: "processing_status", isProcessing: false });

      if (messageId) {
        const status = "success" in event && event.success ? "completed" : "failed";
        await this.pool.query(
          `UPDATE messages SET status = $1, completed_at = $2 WHERE id = $3`,
          [status, Date.now(), messageId]
        );
      }

      // Update session status
      await this.updateSessionField(instance.sessionId, "status", "active");
    }

    // Handle push events
    if (event.type === "push_complete" && "branchName" in event) {
      await this.updateSessionField(
        instance.sessionId,
        "branch_name",
        event.branchName
      );
    }

    // Handle artifacts
    if (event.type === "artifact" && "artifactType" in event) {
      const artifactEvent = event as {
        type: "artifact";
        artifactType: string;
        url: string;
        metadata?: Record<string, unknown>;
        timestamp: number;
      };
      const artifactId = generateId();
      await this.pool.query(
        `INSERT INTO artifacts (id, session_id, type, url, metadata, created_at)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [
          artifactId,
          instance.sessionId,
          artifactEvent.artifactType,
          artifactEvent.url,
          JSON.stringify(artifactEvent.metadata || {}),
          artifactEvent.timestamp,
        ]
      );
      this.broadcast(instance, {
        type: "artifact_created",
        artifact: {
          id: artifactId,
          type: artifactEvent.artifactType,
          url: artifactEvent.url,
        },
      });
    }
  }

  private async spawnSandbox(instance: SessionInstance): Promise<void> {
    this.broadcast(instance, { type: "sandbox_spawning" });
    await this.updateSessionField(instance.sessionId, "sandbox_status", "spawning");

    // Get session details from DB
    const sessionRow = await this.pool.query(
      `SELECT repo_owner, repo_name, repo_id, model, reasoning_effort,
              sandbox_id, sandbox_auth_token
       FROM sessions WHERE id = $1`,
      [instance.sessionId]
    );

    if (sessionRow.rows.length === 0) {
      logger.error("Session not found for sandbox spawn", {
        session_id: instance.sessionId,
      });
      return;
    }

    const session = sessionRow.rows[0];

    // Generate sandbox ID and auth token if not already set
    const sandboxId = session.sandbox_id || generateId();
    const sandboxAuthToken = session.sandbox_auth_token || crypto.randomUUID();

    // Update session with sandbox details
    await this.pool.query(
      `UPDATE sessions SET sandbox_id = $1, sandbox_auth_token = $2 WHERE id = $3`,
      [sandboxId, sandboxAuthToken, instance.sessionId]
    );

    // Get user env vars (repo secrets)
    const userEnvVars = await this.getUserEnvVars(
      session.repo_id,
      session.repo_owner,
      session.repo_name
    );

    // Generate GitHub App token for git operations
    let githubAppToken: string | undefined;
    const appConfig = getGitHubAppConfig({
      githubAppId: this.config.githubAppId,
      githubAppPrivateKey: this.config.githubAppPrivateKey,
      githubAppInstallationId: this.config.githubAppInstallationId,
    });
    if (appConfig) {
      try {
        githubAppToken = await generateInstallationToken(appConfig);
      } catch (err) {
        logger.error("Failed to generate GitHub App token", {
          error: err instanceof Error ? err : String(err),
        });
      }
    }

    // Add system env vars
    const envVars: Record<string, string> = {
      ...userEnvVars,
      ...(githubAppToken ? { GITHUB_APP_TOKEN: githubAppToken } : {}),
    };

    try {
      const result = await this.sandboxProvider.createSandbox({
        sessionId: instance.sessionId,
        sandboxId,
        repoOwner: session.repo_owner,
        repoName: session.repo_name,
        controlPlaneUrl: this.config.workerUrl,
        sandboxAuthToken,
        provider: "anthropic",
        model: session.model || "claude-sonnet-4-5",
        userEnvVars: envVars,
      });

      instance.sandboxActorId = result.providerObjectId || null;
      logger.info("Sandbox spawn initiated", {
        session_id: instance.sessionId,
        sandbox_id: sandboxId,
        actor_id: result.providerObjectId,
      });
    } catch (err) {
      logger.error("Failed to spawn sandbox", {
        session_id: instance.sessionId,
        error: err instanceof Error ? err : String(err),
      });
      this.broadcast(instance, {
        type: "sandbox_error",
        error: "Failed to create sandbox",
      });
      await this.updateSessionField(instance.sessionId, "sandbox_status", "failed");
    }
  }

  private async getUserEnvVars(
    repoId: number,
    repoOwner: string,
    repoName: string
  ): Promise<Record<string, string>> {
    if (!this.config.repoSecretsEncryptionKey) return {};

    try {
      const result = await this.pool.query(
        `SELECT key, encrypted_value, iv FROM repo_secrets WHERE repo_id = $1`,
        [repoId]
      );

      const vars: Record<string, string> = {};
      for (const row of result.rows) {
        try {
          const decipher = crypto.createDecipheriv(
            "aes-256-gcm",
            Buffer.from(this.config.repoSecretsEncryptionKey, "hex"),
            Buffer.from(row.iv, "hex")
          );
          // The encrypted_value contains ciphertext + auth tag
          const encrypted = Buffer.from(row.encrypted_value, "hex");
          const authTag = encrypted.subarray(encrypted.length - 16);
          const ciphertext = encrypted.subarray(0, encrypted.length - 16);
          decipher.setAuthTag(authTag);
          let decrypted = decipher.update(ciphertext, undefined, "utf8");
          decrypted += decipher.final("utf8");
          vars[row.key] = decrypted;
        } catch (err) {
          logger.error("Failed to decrypt repo secret", {
            key: row.key,
            repo_owner: repoOwner,
            repo_name: repoName,
            error: err instanceof Error ? err : String(err),
          });
        }
      }

      return vars;
    } catch (err) {
      logger.error("Failed to load repo secrets", {
        repo_id: repoId,
        error: err instanceof Error ? err : String(err),
      });
      return {};
    }
  }

  private async storeEvent(
    sessionId: string,
    event: SandboxEvent,
    messageId?: string | null
  ): Promise<void> {
    const id = generateId();
    const timestamp = "timestamp" in event ? event.timestamp : Date.now();

    await this.pool.query(
      `INSERT INTO events (id, session_id, type, data, message_id, created_at)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [id, sessionId, event.type, JSON.stringify(event), messageId || null, timestamp]
    );
  }

  private async replayEvents(
    instance: SessionInstance,
    ws: WebSocket
  ): Promise<void> {
    const result = await this.pool.query(
      `SELECT id, type, data, message_id, created_at FROM events
       WHERE session_id = $1
       ORDER BY created_at DESC, id DESC
       LIMIT $2`,
      [instance.sessionId, REPLAY_PAGE_SIZE + 1]
    );

    const hasMore = result.rows.length > REPLAY_PAGE_SIZE;
    const items = result.rows.slice(0, REPLAY_PAGE_SIZE).reverse();

    // Send each event
    for (const row of items) {
      const event = {
        ...row.data,
        type: row.type,
        messageId: row.message_id,
        timestamp: Number(row.created_at),
      } as SandboxEvent;
      this.send(ws, { type: "sandbox_event", event });
    }

    const cursor =
      items.length > 0
        ? { timestamp: Number(items[0].created_at), id: items[0].id as string }
        : null;

    this.send(ws, { type: "replay_complete", hasMore, cursor });
  }

  async getSessionState(sessionId: string): Promise<SessionState> {
    const result = await this.pool.query(
      `SELECT id, title, repo_owner, repo_name, branch_name, status,
              sandbox_status, model, reasoning_effort, created_at
       FROM sessions WHERE id = $1`,
      [sessionId]
    );

    if (result.rows.length === 0) {
      return {
        id: sessionId,
        title: null,
        repoOwner: "",
        repoName: "",
        branchName: null,
        status: "created",
        sandboxStatus: "pending",
        messageCount: 0,
        createdAt: Date.now(),
        isProcessing: false,
      };
    }

    const row = result.rows[0];
    const instance = this.sessions.get(sessionId);

    // Count messages
    const msgCount = await this.pool.query(
      `SELECT COUNT(*) as count FROM messages WHERE session_id = $1`,
      [sessionId]
    );

    return {
      id: row.id,
      title: row.title,
      repoOwner: row.repo_owner,
      repoName: row.repo_name,
      branchName: row.branch_name,
      status: row.status,
      sandboxStatus: row.sandbox_status || "pending",
      messageCount: parseInt(msgCount.rows[0].count, 10),
      createdAt: Number(row.created_at),
      model: row.model,
      reasoningEffort: row.reasoning_effort,
      isProcessing: instance?.processingMessageId != null,
    };
  }

  /**
   * Generate a WebSocket authentication token.
   */
  async generateWsToken(
    sessionId: string,
    userId: string,
    githubLogin?: string,
    githubName?: string,
    githubEmail?: string,
    githubTokenEncrypted?: string | null,
    githubRefreshTokenEncrypted?: string | null,
    githubTokenExpiresAt?: number | null
  ): Promise<string> {
    const token = crypto.randomUUID();
    const tokenHash = crypto
      .createHash("sha256")
      .update(token)
      .digest("hex");
    const now = Date.now();

    await this.pool.query(
      `INSERT INTO ws_tokens (token_hash, session_id, user_id, github_login, github_name,
                              github_email, github_token_encrypted, github_refresh_token_encrypted,
                              github_token_expires_at, created_at, expires_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
      [
        tokenHash,
        sessionId,
        userId,
        githubLogin || null,
        githubName || null,
        githubEmail || null,
        githubTokenEncrypted || null,
        githubRefreshTokenEncrypted || null,
        githubTokenExpiresAt || null,
        now,
        now + WS_TOKEN_EXPIRY_MS,
      ]
    );

    return token;
  }

  private async ensureParticipant(
    sessionId: string,
    userId: string,
    githubLogin?: string,
    githubName?: string,
    githubEmail?: string,
    githubTokenEncrypted?: string | null,
    githubRefreshTokenEncrypted?: string | null,
    githubTokenExpiresAt?: number | null
  ): Promise<string> {
    // Check if participant already exists
    const existing = await this.pool.query(
      `SELECT id FROM participants WHERE session_id = $1 AND user_id = $2`,
      [sessionId, userId]
    );

    if (existing.rows.length > 0) {
      // Update token info
      await this.pool.query(
        `UPDATE participants SET
         github_token_encrypted = COALESCE($1, github_token_encrypted),
         github_refresh_token_encrypted = COALESCE($2, github_refresh_token_encrypted),
         github_token_expires_at = COALESCE($3, github_token_expires_at)
         WHERE id = $4`,
        [
          githubTokenEncrypted || null,
          githubRefreshTokenEncrypted || null,
          githubTokenExpiresAt || null,
          existing.rows[0].id,
        ]
      );
      return existing.rows[0].id;
    }

    // Create new participant
    const participantId = generateId();
    await this.pool.query(
      `INSERT INTO participants (id, session_id, user_id, github_login, github_name,
                                 github_email, github_token_encrypted,
                                 github_refresh_token_encrypted, github_token_expires_at,
                                 role, joined_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'member', $10)`,
      [
        participantId,
        sessionId,
        userId,
        githubLogin || null,
        githubName || null,
        githubEmail || null,
        githubTokenEncrypted || null,
        githubRefreshTokenEncrypted || null,
        githubTokenExpiresAt || null,
        Date.now(),
      ]
    );

    return participantId;
  }

  private handleClientDisconnect(instance: SessionInstance, ws: WebSocket): void {
    // Find and remove the client
    for (const [clientId, client] of instance.clients) {
      if (client.ws === ws) {
        instance.clients.delete(clientId);
        logger.info("Client disconnected", {
          session_id: instance.sessionId,
          client_id: clientId,
          user_id: client.userId,
        });
        break;
      }
    }

    // Broadcast updated presence
    this.broadcastPresence(instance);

    // If no clients left, start inactivity timer
    if (instance.clients.size === 0) {
      this.resetInactivityTimer(instance);
    }
  }

  private broadcast(instance: SessionInstance, msg: ServerMessage): void {
    const data = JSON.stringify(msg);
    for (const client of instance.clients.values()) {
      if (client.ws.readyState === 1) {
        // WebSocket.OPEN
        client.ws.send(data);
      }
    }
  }

  private send(ws: WebSocket, msg: ServerMessage): void {
    if (ws.readyState === 1) {
      ws.send(JSON.stringify(msg));
    }
  }

  private broadcastPresence(instance: SessionInstance): void {
    const participants: ParticipantPresence[] = [];
    for (const client of instance.clients.values()) {
      participants.push({
        participantId: client.participantId,
        userId: client.userId,
        name: client.name,
        avatar: client.avatar,
        status: client.status,
        lastSeen: client.lastSeen,
      });
    }
    this.broadcast(instance, { type: "presence_sync", participants });
  }

  private startHeartbeatMonitor(instance: SessionInstance): void {
    this.clearHeartbeatMonitor(instance);

    instance.heartbeatTimer = setInterval(() => {
      const elapsed = Date.now() - instance.lastHeartbeat;
      if (elapsed > HEARTBEAT_TIMEOUT_MS) {
        logger.warn("Sandbox heartbeat timeout", {
          session_id: instance.sessionId,
          elapsed_ms: elapsed,
        });
        this.broadcast(instance, { type: "sandbox_status", status: "stale" });
        this.updateSessionField(instance.sessionId, "sandbox_status", "stale");
      }
    }, 30_000);
  }

  private clearHeartbeatMonitor(instance: SessionInstance): void {
    if (instance.heartbeatTimer) {
      clearInterval(instance.heartbeatTimer);
      instance.heartbeatTimer = null;
    }
  }

  private resetInactivityTimer(instance: SessionInstance): void {
    if (instance.inactivityTimer) {
      clearTimeout(instance.inactivityTimer);
    }

    instance.inactivityTimer = setTimeout(async () => {
      if (instance.clients.size === 0 && instance.sandboxActorId) {
        logger.info("Inactivity timeout - destroying sandbox", {
          session_id: instance.sessionId,
          actor_id: instance.sandboxActorId,
        });

        if (instance.sandboxWs) {
          instance.sandboxWs.close();
          instance.sandboxWs = null;
        }

        await this.sandboxProvider.destroySandbox(instance.sandboxActorId);
        instance.sandboxActorId = null;
        await this.updateSessionField(instance.sessionId, "sandbox_status", "stopped");

        // Clean up the in-memory instance
        this.clearHeartbeatMonitor(instance);
        this.sessions.delete(instance.sessionId);
      }
    }, this.config.sandboxInactivityTimeoutMs);
  }

  private async updateSessionField(
    sessionId: string,
    field: string,
    value: string
  ): Promise<void> {
    // Only allow known fields
    const allowedFields = [
      "status",
      "sandbox_status",
      "branch_name",
      "model",
      "current_sha",
      "base_sha",
    ];
    if (!allowedFields.includes(field)) return;

    await this.pool.query(
      `UPDATE sessions SET ${field} = $1, updated_at = $2 WHERE id = $3`,
      [value, Date.now(), sessionId]
    );
  }

  /**
   * Create a new session.
   */
  async createSession(params: {
    sessionId: string;
    repoOwner: string;
    repoName: string;
    repoId: number;
    title?: string;
    model?: string;
    reasoningEffort?: string;
    userId: string;
    githubLogin?: string;
    githubName?: string;
    githubEmail?: string;
    githubTokenEncrypted?: string | null;
  }): Promise<void> {
    const now = Date.now();

    await this.pool.query(
      `INSERT INTO sessions (id, title, repo_owner, repo_name, repo_id, model, reasoning_effort,
                             status, sandbox_status, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'created', 'pending', $8, $8)`,
      [
        params.sessionId,
        params.title || null,
        params.repoOwner,
        params.repoName,
        params.repoId,
        params.model || "claude-haiku-4-5",
        params.reasoningEffort || null,
        now,
      ]
    );

    // Create initial participant (owner)
    await this.ensureParticipant(
      params.sessionId,
      params.userId,
      params.githubLogin,
      params.githubName,
      params.githubEmail,
      params.githubTokenEncrypted
    );
  }

  /**
   * Verify a sandbox auth token for a session.
   */
  async verifySandboxToken(sessionId: string, token: string): Promise<boolean> {
    const result = await this.pool.query(
      `SELECT sandbox_auth_token FROM sessions WHERE id = $1`,
      [sessionId]
    );

    if (result.rows.length === 0) return false;
    return result.rows[0].sandbox_auth_token === token;
  }

  /**
   * Get session details for API responses.
   */
  async getSession(sessionId: string): Promise<Record<string, unknown> | null> {
    const result = await this.pool.query(
      `SELECT * FROM sessions WHERE id = $1`,
      [sessionId]
    );

    if (result.rows.length === 0) return null;

    const row = result.rows[0];
    return {
      id: row.id,
      title: row.title,
      repoOwner: row.repo_owner,
      repoName: row.repo_name,
      repoDefaultBranch: row.repo_default_branch || "main",
      branchName: row.branch_name,
      baseSha: row.base_sha,
      currentSha: row.current_sha,
      opencodeSessionId: row.opencode_session_id,
      status: row.status,
      createdAt: Number(row.created_at),
      updatedAt: Number(row.updated_at),
    };
  }

  /**
   * Clean up expired WS tokens periodically.
   */
  async cleanupExpiredTokens(): Promise<void> {
    await this.pool.query(
      `DELETE FROM ws_tokens WHERE expires_at < $1`,
      [Date.now()]
    );
  }

  /**
   * Graceful shutdown - close all WebSocket connections.
   */
  async shutdown(): Promise<void> {
    for (const instance of this.sessions.values()) {
      for (const client of instance.clients.values()) {
        client.ws.close(1001, "Server shutting down");
      }
      if (instance.sandboxWs) {
        instance.sandboxWs.close(1001, "Server shutting down");
      }
      this.clearHeartbeatMonitor(instance);
      if (instance.inactivityTimer) {
        clearTimeout(instance.inactivityTimer);
      }
    }
    this.sessions.clear();
  }
}
