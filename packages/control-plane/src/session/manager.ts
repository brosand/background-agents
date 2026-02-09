/**
 * Session Manager for Open-Inspect Control Plane.
 *
 * Manages in-memory session state backed by PostgreSQL. Each session has a
 * SessionInstance that holds client WebSocket connections and a sandbox-agent
 * SDK client for controlling the coding agent running inside the sandbox container.
 *
 * Communication with sandboxes uses the sandbox-agent HTTP/SSE API (via the
 * `sandbox-agent` npm package) instead of raw WebSockets. Events from the
 * sandbox-agent's universal event schema are mapped to our SandboxEvent format
 * for frontend compatibility.
 */

import type { Pool } from "pg";
import type WebSocket from "ws";
import crypto from "node:crypto";
import { SandboxAgent } from "sandbox-agent";
import type { UniversalEvent } from "sandbox-agent";
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
const WS_TOKEN_EXPIRY_MS = 5 * 60 * 1000; // 5 minutes
const REPLAY_PAGE_SIZE = 200;
const SANDBOX_AGENT_HEALTH_POLL_MS = 2_000;
const SANDBOX_AGENT_HEALTH_TIMEOUT_MS = 60_000;

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
  sandboxAgent: SandboxAgent | null;
  sandboxActorId: string | null;
  sandboxSessionId: string | null;
  sandboxEventAbort: AbortController | null;
  processingMessageId: string | null;
  inactivityTimer: ReturnType<typeof setTimeout> | null;
  lastEventSequence: number;
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
        sandboxAgent: null,
        sandboxActorId: null,
        sandboxSessionId: null,
        sandboxEventAbort: null,
        processingMessageId: null,
        inactivityTimer: null,
        lastEventSequence: 0,
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

    // Ensure sandbox is running and connected
    if (!instance.sandboxAgent) {
      await this.spawnSandbox(instance);
    }

    // Send prompt to sandbox via sandbox-agent SDK
    if (instance.sandboxAgent && instance.sandboxSessionId) {
      instance.processingMessageId = messageId;
      this.broadcast(instance, { type: "processing_status", isProcessing: true });

      try {
        await instance.sandboxAgent.postMessage(instance.sandboxSessionId, {
          message: msg.content,
        });

        await this.pool.query(
          `UPDATE messages SET status = 'processing', started_at = $1 WHERE id = $2`,
          [Date.now(), messageId]
        );
      } catch (err) {
        logger.error("Failed to send message to sandbox-agent", {
          session_id: instance.sessionId,
          error: err instanceof Error ? err : String(err),
        });
        this.send(ws, {
          type: "error",
          code: "sandbox_send_failed",
          message: "Failed to send message to sandbox",
        });
        instance.processingMessageId = null;
        this.broadcast(instance, { type: "processing_status", isProcessing: false });
      }
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
    if (instance.sandboxAgent && instance.sandboxSessionId) {
      try {
        await instance.sandboxAgent.terminateSession(instance.sandboxSessionId);
      } catch (err) {
        logger.error("Failed to terminate sandbox session", {
          session_id: instance.sessionId,
          error: err instanceof Error ? err : String(err),
        });
      }
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

  /**
   * Map a sandbox-agent UniversalEvent to our SandboxEvent format.
   *
   * The sandbox-agent emits events in a standardized schema (UniversalEvent)
   * that works across all supported agents (Claude Code, Codex, OpenCode, Amp).
   * We map these to our existing SandboxEvent format for frontend compatibility.
   */
  private mapUniversalEvent(
    event: UniversalEvent,
    sandboxId: string,
    messageId: string | null
  ): SandboxEvent[] {
    const now = new Date(event.time).getTime() || Date.now();
    const events: SandboxEvent[] = [];

    switch (event.type) {
      case "item.delta": {
        // Streaming text delta → token event
        const delta = event.data as { delta: string; item_id: string };
        if (delta.delta) {
          events.push({
            type: "token",
            content: delta.delta,
            messageId: messageId || "",
            sandboxId,
            timestamp: now,
          });
        }
        break;
      }

      case "item.started":
      case "item.completed": {
        const itemData = event.data as {
          item: {
            item_id: string;
            kind: string;
            content: Array<{ type: string; text?: string; name?: string; arguments?: string; call_id?: string; output?: string; path?: string; action?: string; diff?: string }>;
            status: string;
            role?: string;
          };
        };
        const item = itemData.item;

        if (item.kind === "tool_call") {
          // Extract tool call details from content parts
          for (const part of item.content) {
            if (part.type === "tool_call") {
              events.push({
                type: "tool_call",
                tool: part.name || "unknown",
                args: { arguments: part.arguments || "" },
                callId: part.call_id || item.item_id,
                messageId: messageId || "",
                sandboxId,
                timestamp: now,
              });
            } else if (part.type === "file_ref") {
              events.push({
                type: "tool_call",
                tool: part.action || "file",
                args: { path: part.path, diff: part.diff },
                callId: item.item_id,
                messageId: messageId || "",
                sandboxId,
                timestamp: now,
              });
            }
          }
        } else if (item.kind === "tool_result" && event.type === "item.completed") {
          for (const part of item.content) {
            if (part.type === "tool_result") {
              events.push({
                type: "tool_result",
                callId: part.call_id || item.item_id,
                result: part.output || "",
                messageId: messageId || "",
                sandboxId,
                timestamp: now,
              });
            } else if (part.type === "text") {
              events.push({
                type: "tool_result",
                callId: item.item_id,
                result: part.text || "",
                messageId: messageId || "",
                sandboxId,
                timestamp: now,
              });
            }
          }
        }
        break;
      }

      case "turn.ended": {
        // Turn ended → execution_complete
        events.push({
          type: "execution_complete",
          messageId: messageId || "",
          success: true,
          sandboxId,
          timestamp: now,
        });
        break;
      }

      case "session.ended": {
        const endData = event.data as {
          reason: string;
          terminated_by: string;
          exit_code?: number;
          message?: string;
        };
        events.push({
          type: "execution_complete",
          messageId: messageId || "",
          success: endData.reason === "completed",
          error: endData.message || undefined,
          sandboxId,
          timestamp: now,
        });
        break;
      }

      case "error": {
        const errorData = event.data as { message: string; code?: string };
        events.push({
          type: "error",
          error: errorData.message,
          messageId: messageId || "",
          sandboxId,
          timestamp: now,
        });
        break;
      }

      case "permission.requested": {
        // Auto-approve permissions for background agent execution
        // This is handled in the event stream loop, not mapped to a frontend event
        break;
      }

      // session.started, turn.started, permission.resolved, question.*, agent.unparsed
      // are not mapped to frontend events (internal lifecycle only)
      default:
        break;
    }

    return events;
  }

  /**
   * Start consuming SSE events from the sandbox-agent for a session.
   * Runs in the background as a long-lived async loop.
   */
  private startEventStream(instance: SessionInstance): void {
    if (!instance.sandboxAgent || !instance.sandboxSessionId) return;

    // Abort any existing stream
    instance.sandboxEventAbort?.abort();
    const abort = new AbortController();
    instance.sandboxEventAbort = abort;

    const agent = instance.sandboxAgent;
    const agentSessionId = instance.sandboxSessionId;
    const sessionId = instance.sessionId;

    // Get sandbox ID from DB for event mapping
    const sandboxIdPromise = this.pool.query(
      `SELECT sandbox_id FROM sessions WHERE id = $1`,
      [sessionId]
    ).then(r => r.rows[0]?.sandbox_id || sessionId);

    (async () => {
      const sandboxId = await sandboxIdPromise;

      try {
        for await (const event of agent.streamEvents(
          agentSessionId,
          { offset: instance.lastEventSequence || undefined },
          abort.signal
        )) {
          // Update last seen sequence for resumption
          instance.lastEventSequence = event.sequence;

          // Auto-approve permission requests
          if (event.type === "permission.requested") {
            const permData = event.data as { permission_id: string };
            try {
              await agent.replyPermission(agentSessionId, permData.permission_id, {
                reply: "once",
              });
            } catch (err) {
              logger.error("Failed to auto-approve permission", {
                session_id: sessionId,
                permission_id: permData.permission_id,
                error: err instanceof Error ? err : String(err),
              });
            }
            continue;
          }

          // Auto-answer questions (reject them since we can't prompt the user)
          if (event.type === "question.requested") {
            const qData = event.data as { question_id: string };
            try {
              await agent.rejectQuestion(agentSessionId, qData.question_id);
            } catch (err) {
              logger.error("Failed to reject question", {
                session_id: sessionId,
                question_id: qData.question_id,
                error: err instanceof Error ? err : String(err),
              });
            }
            continue;
          }

          // Map to our event format
          const mappedEvents = this.mapUniversalEvent(
            event,
            sandboxId,
            instance.processingMessageId
          );

          for (const sandboxEvent of mappedEvents) {
            // Store event in DB
            await this.storeEvent(sessionId, sandboxEvent, instance.processingMessageId);

            // Broadcast to all clients
            this.broadcast(instance, { type: "sandbox_event", event: sandboxEvent });

            // Handle completion
            if (sandboxEvent.type === "execution_complete") {
              const currentMessageId = instance.processingMessageId;
              instance.processingMessageId = null;
              this.broadcast(instance, { type: "processing_status", isProcessing: false });

              if (currentMessageId) {
                const status = sandboxEvent.success ? "completed" : "failed";
                await this.pool.query(
                  `UPDATE messages SET status = $1, completed_at = $2 WHERE id = $3`,
                  [status, Date.now(), currentMessageId]
                );
              }

              await this.updateSessionField(sessionId, "status", "active");
            }
          }

          // Handle session ended - clean up the agent connection
          if (event.type === "session.ended") {
            logger.info("Sandbox session ended", {
              session_id: sessionId,
              reason: (event.data as { reason: string }).reason,
            });
            this.broadcast(instance, { type: "sandbox_status", status: "stopped" });
            await this.updateSessionField(sessionId, "sandbox_status", "stopped");
            break;
          }
        }
      } catch (err) {
        if (abort.signal.aborted) {
          logger.info("Event stream aborted", { session_id: sessionId });
          return;
        }
        logger.error("Event stream error", {
          session_id: sessionId,
          error: err instanceof Error ? err : String(err),
        });
        this.broadcast(instance, { type: "sandbox_status", status: "stale" });
        await this.updateSessionField(sessionId, "sandbox_status", "stale");
      }
    })();
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

      if (!result.sandboxUrl || !result.sandboxAgentToken) {
        throw new Error("Sandbox created but no URL or token returned");
      }

      logger.info("Sandbox created, waiting for sandbox-agent health", {
        session_id: instance.sessionId,
        sandbox_id: sandboxId,
        actor_id: result.providerObjectId,
        sandbox_url: result.sandboxUrl,
      });

      // Wait for sandbox-agent to become healthy
      await this.waitForSandboxHealth(result.sandboxUrl, result.sandboxAgentToken);

      // Connect to sandbox-agent via SDK
      const agent = await SandboxAgent.connect({
        baseUrl: result.sandboxUrl,
        token: result.sandboxAgentToken,
      });

      instance.sandboxAgent = agent;

      // Create a sandbox-agent session with Claude Code
      const agentSessionId = `session-${instance.sessionId}`;
      const createResult = await agent.createSession(agentSessionId, {
        agent: "claude-code",
        directory: "/workspace",
        permissionMode: "auto",
        model: session.model || undefined,
      });

      if (!createResult.healthy) {
        throw new Error(
          `sandbox-agent session creation failed: ${createResult.error?.message || "unhealthy"}`
        );
      }

      instance.sandboxSessionId = agentSessionId;

      // Start consuming SSE events from the sandbox
      this.startEventStream(instance);

      // Notify clients
      this.broadcast(instance, { type: "sandbox_ready" });
      await this.updateSessionField(instance.sessionId, "sandbox_status", "ready");

      logger.info("Sandbox agent connected", {
        session_id: instance.sessionId,
        sandbox_id: sandboxId,
        agent_session_id: agentSessionId,
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

  /**
   * Poll sandbox-agent health endpoint until it responds.
   */
  private async waitForSandboxHealth(
    sandboxUrl: string,
    token: string
  ): Promise<void> {
    const deadline = Date.now() + SANDBOX_AGENT_HEALTH_TIMEOUT_MS;

    while (Date.now() < deadline) {
      try {
        const response = await fetch(`${sandboxUrl}/v1/health`, {
          headers: { Authorization: `Bearer ${token}` },
          signal: AbortSignal.timeout(5_000),
        });
        if (response.ok) return;
      } catch {
        // Expected while container is starting up
      }
      await new Promise((resolve) => setTimeout(resolve, SANDBOX_AGENT_HEALTH_POLL_MS));
    }

    throw new Error("Sandbox agent health check timed out");
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

        // Abort the SSE stream
        instance.sandboxEventAbort?.abort();
        instance.sandboxEventAbort = null;

        // Dispose the sandbox-agent SDK client
        if (instance.sandboxAgent) {
          try {
            await instance.sandboxAgent.dispose();
          } catch {
            // Ignore dispose errors
          }
          instance.sandboxAgent = null;
        }
        instance.sandboxSessionId = null;

        await this.sandboxProvider.destroySandbox(instance.sandboxActorId);
        instance.sandboxActorId = null;
        await this.updateSessionField(instance.sessionId, "sandbox_status", "stopped");

        // Clean up the in-memory instance
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
   * Graceful shutdown - close all connections.
   */
  async shutdown(): Promise<void> {
    for (const instance of this.sessions.values()) {
      // Close client WebSockets
      for (const client of instance.clients.values()) {
        client.ws.close(1001, "Server shutting down");
      }

      // Abort SSE streams
      instance.sandboxEventAbort?.abort();

      // Dispose sandbox-agent clients
      if (instance.sandboxAgent) {
        try {
          await instance.sandboxAgent.dispose();
        } catch {
          // Ignore dispose errors during shutdown
        }
      }

      if (instance.inactivityTimer) {
        clearTimeout(instance.inactivityTimer);
      }
    }
    this.sessions.clear();
  }
}
