/**
 * Session Actor Manager
 *
 * Replaces Cloudflare Durable Objects for per-session state management.
 * Uses Rivet actors conceptually, but implemented as an in-process actor
 * system that can be upgraded to Rivet's distributed actors when deploying
 * on Rivet Cloud or with RivetKit on K8s.
 *
 * Each session has a lightweight "actor" that manages:
 * - WebSocket connections for real-time events
 * - Sandbox lifecycle (spawn, monitor, snapshot)
 * - Message processing queue
 * - Heartbeat monitoring
 *
 * In production, this can be partitioned across multiple control-plane replicas
 * using Rivet's partition topology with Redis coordination.
 */

import { WebSocket as HonoWebSocket } from "@hono/node-ws";
import { generateId, hashToken } from "../auth/crypto";
import { PgSessionRepository } from "../session/pg-repository";
import { createK8sProvider } from "../sandbox/providers/k8s-provider";
import { createLogger } from "../logger";
import type { ServerConfig } from "../server";
import type { ClientMessage, ServerMessage, SandboxEvent, SessionState } from "../types";

const logger = createLogger("session-actor");

interface SessionActor {
  sessionId: string;
  clients: Map<string, { ws: HonoWebSocket; participantId: string; userId: string; name: string }>;
  sandboxWs: WebSocket | null;
  heartbeatTimer: ReturnType<typeof setInterval> | null;
  inactivityTimer: ReturnType<typeof setTimeout> | null;
  isProcessing: boolean;
}

export class SessionActorManager {
  private actors: Map<string, SessionActor> = new Map();
  private repo: PgSessionRepository;
  private config: ServerConfig;
  private sandboxProvider;

  constructor(repo: PgSessionRepository, config: ServerConfig) {
    this.repo = repo;
    this.config = config;
    this.sandboxProvider = createK8sProvider(config.sandboxApiUrl, config.apiSecret);
  }

  /**
   * Get or create an actor for a session.
   */
  private getActor(sessionId: string): SessionActor {
    let actor = this.actors.get(sessionId);
    if (!actor) {
      actor = {
        sessionId,
        clients: new Map(),
        sandboxWs: null,
        heartbeatTimer: null,
        inactivityTimer: null,
        isProcessing: false,
      };
      this.actors.set(sessionId, actor);
    }
    return actor;
  }

  /**
   * Initialize a session actor (called on session creation).
   */
  initSession(sessionId: string): void {
    this.getActor(sessionId);
  }

  /**
   * Destroy a session actor.
   */
  destroySession(sessionId: string): void {
    const actor = this.actors.get(sessionId);
    if (!actor) return;

    // Clear timers
    if (actor.heartbeatTimer) clearInterval(actor.heartbeatTimer);
    if (actor.inactivityTimer) clearTimeout(actor.inactivityTimer);

    // Close all WebSocket connections
    for (const [, client] of actor.clients) {
      try { client.ws.close(); } catch {}
    }

    // Close sandbox WebSocket
    if (actor.sandboxWs) {
      try { actor.sandboxWs.close(); } catch {}
    }

    this.actors.delete(sessionId);
  }

  /**
   * Handle a new WebSocket connection.
   */
  handleWsConnect(sessionId: string, ws: HonoWebSocket): void {
    const actor = this.getActor(sessionId);
    const clientId = generateId();
    // The client will authenticate via a `subscribe` message
    actor.clients.set(clientId, {
      ws,
      participantId: "",
      userId: "",
      name: "",
    });

    // Store client ID on the WebSocket for later lookup
    (ws as any).__clientId = clientId;
  }

  /**
   * Handle a WebSocket message from a client.
   */
  async handleWsMessage(sessionId: string, ws: HonoWebSocket, data: string): Promise<void> {
    const actor = this.getActor(sessionId);
    const clientId = (ws as any).__clientId as string;

    let msg: ClientMessage;
    try {
      msg = JSON.parse(data);
    } catch {
      this.sendToWs(ws, { type: "error", code: "INVALID_JSON", message: "Invalid JSON" });
      return;
    }

    switch (msg.type) {
      case "ping":
        this.sendToWs(ws, { type: "pong", timestamp: Date.now() });
        break;

      case "subscribe":
        await this.handleSubscribe(actor, clientId, ws, msg.token, msg.clientId);
        break;

      case "prompt":
        await this.handleClientPrompt(actor, ws, msg);
        break;

      case "stop":
        await this.stopExecution(sessionId);
        break;

      case "typing":
        await this.handleTyping(actor);
        break;

      case "fetch_history":
        await this.handleFetchHistory(actor, ws, msg);
        break;

      default:
        break;
    }
  }

  /**
   * Handle WebSocket disconnect.
   */
  handleWsDisconnect(sessionId: string, ws: HonoWebSocket): void {
    const actor = this.actors.get(sessionId);
    if (!actor) return;

    const clientId = (ws as any).__clientId as string;
    if (clientId) {
      actor.clients.delete(clientId);
    }

    // If no more clients, start inactivity timer
    if (actor.clients.size === 0) {
      this.startInactivityTimer(actor);
    }
  }

  /**
   * Handle subscribe message - authenticate client.
   */
  private async handleSubscribe(
    actor: SessionActor,
    clientId: string,
    ws: HonoWebSocket,
    token: string,
    subscriberClientId: string
  ): Promise<void> {
    const tokenHash = await hashToken(token);
    const participant = await this.repo.getParticipantByWsTokenHash(actor.sessionId, tokenHash);

    if (!participant) {
      this.sendToWs(ws, { type: "error", code: "AUTH_FAILED", message: "Invalid token" });
      ws.close();
      return;
    }

    // Update client info
    const client = actor.clients.get(clientId);
    if (client) {
      client.participantId = participant.id as string;
      client.userId = participant.user_id as string;
      client.name = (participant.github_name || participant.github_login || "Unknown") as string;
    }

    // Save WS client mapping for recovery
    await this.repo.upsertWsClientMapping(
      actor.sessionId,
      clientId,
      participant.id as string,
      subscriberClientId
    );

    // Build session state for the subscribed response
    const session = await this.repo.getSession(actor.sessionId);
    const sandbox = await this.repo.getSandbox(actor.sessionId);
    const messageCount = await this.repo.getMessageCount(actor.sessionId);

    const state: SessionState = {
      id: actor.sessionId,
      title: session?.title as string | null,
      repoOwner: session?.repo_owner as string,
      repoName: session?.repo_name as string,
      branchName: session?.branch_name as string | null,
      status: (session?.status as any) || "created",
      sandboxStatus: (sandbox?.status as any) || "pending",
      messageCount,
      createdAt: session?.created_at as number,
      model: session?.model as string | undefined,
      reasoningEffort: session?.reasoning_effort as string | undefined,
      isProcessing: actor.isProcessing,
    };

    this.sendToWs(ws, {
      type: "subscribed",
      sessionId: actor.sessionId,
      state,
      participantId: participant.id as string,
      participant: {
        participantId: participant.id as string,
        name: client?.name || "Unknown",
      },
    });

    // Send recent events for replay
    const recentEvents = await this.repo.getEventsForReplay(actor.sessionId, 500);
    const reversedEvents = (recentEvents as any[]).reverse();
    for (const event of reversedEvents) {
      this.sendToWs(ws, {
        type: "sandbox_event",
        event: JSON.parse(event.data),
      });
    }

    this.sendToWs(ws, {
      type: "replay_complete",
      hasMore: recentEvents.length >= 500,
      cursor: recentEvents.length > 0
        ? { timestamp: (recentEvents[0] as any).created_at, id: (recentEvents[0] as any).id }
        : null,
    });
  }

  /**
   * Handle client prompt message.
   */
  private async handleClientPrompt(
    actor: SessionActor,
    ws: HonoWebSocket,
    msg: Extract<ClientMessage, { type: "prompt" }>
  ): Promise<void> {
    const client = [...actor.clients.values()].find((c) => c.ws === ws);
    if (!client || !client.participantId) {
      this.sendToWs(ws, { type: "error", code: "NOT_SUBSCRIBED", message: "Not subscribed" });
      return;
    }

    const messageId = generateId();
    await this.repo.createMessage({
      id: messageId,
      sessionId: actor.sessionId,
      authorId: client.participantId,
      content: msg.content,
      source: "web",
      model: msg.model,
      reasoningEffort: msg.reasoningEffort,
    });

    const pendingCount = await this.repo.getPendingOrProcessingCount(actor.sessionId);
    this.sendToWs(ws, { type: "prompt_queued", messageId, position: pendingCount });

    // Trigger processing
    await this.processNextMessage(actor);
  }

  /**
   * Enqueue a prompt from the REST API.
   */
  async enqueuePrompt(sessionId: string, messageId: string): Promise<void> {
    const actor = this.getActor(sessionId);

    // Broadcast to all connected clients
    this.broadcast(actor, {
      type: "prompt_queued",
      messageId,
      position: await this.repo.getPendingOrProcessingCount(sessionId),
    });

    await this.processNextMessage(actor);
  }

  /**
   * Process the next pending message.
   */
  private async processNextMessage(actor: SessionActor): Promise<void> {
    if (actor.isProcessing) return;

    const message = await this.repo.getNextPendingMessage(actor.sessionId);
    if (!message) return;

    actor.isProcessing = true;
    await this.repo.updateMessageToProcessing(message.id as string);

    // Update session status
    await this.repo.updateSessionStatus(actor.sessionId, "active");

    // Ensure sandbox is running
    const sandbox = await this.repo.getSandbox(actor.sessionId);
    if (!sandbox || sandbox.status === "pending" || sandbox.status === "stopped" || sandbox.status === "failed") {
      await this.spawnSandbox(actor);
    }

    // The sandbox bridge will pick up the message via WebSocket
    this.broadcast(actor, { type: "processing_status", isProcessing: true });
  }

  /**
   * Spawn a sandbox pod for the session.
   */
  private async spawnSandbox(actor: SessionActor): Promise<void> {
    this.broadcast(actor, { type: "sandbox_spawning" });

    const session = await this.repo.getSession(actor.sessionId);
    if (!session) return;

    const sandboxId = `sandbox-${actor.sessionId}-${Date.now()}`;
    const sandboxAuthToken = generateId();
    const tokenHash = await hashToken(sandboxAuthToken);

    try {
      // Update sandbox record
      await this.repo.updateSandboxForSpawn(actor.sessionId, {
        sandboxId,
        authToken: tokenHash,
        status: "spawning",
      });

      // Get user env vars (repo secrets)
      let userEnvVars: Record<string, string> | undefined;
      if (this.config.repoSecretsKey) {
        const secretsStore = new (await import("../db/pg-repo-secrets")).PgRepoSecretsStore(
          this.config.repoSecretsKey
        );
        userEnvVars = await secretsStore.getDecryptedSecrets(
          session.repo_owner as string,
          session.repo_name as string
        );
      }

      const result = await this.sandboxProvider.createSandbox({
        sessionId: actor.sessionId,
        sandboxId,
        repoOwner: session.repo_owner as string,
        repoName: session.repo_name as string,
        controlPlaneUrl: this.config.workerUrl || `http://localhost:${this.config.port}`,
        sandboxAuthToken,
        provider: "anthropic",
        model: (session.model || "claude-sonnet-4-5") as string,
        userEnvVars,
      });

      // Update with provider info
      if (result.providerObjectId) {
        await this.repo.updateSandboxProviderObjectId(actor.sessionId, result.providerObjectId);
      }

      await this.repo.updateSandboxStatus(actor.sessionId, "connecting");
      this.broadcast(actor, { type: "sandbox_status", status: "connecting" });

      // Start heartbeat monitoring
      this.startHeartbeatMonitor(actor);

      logger.info("Sandbox spawned", {
        session_id: actor.sessionId,
        sandbox_id: sandboxId,
      });
    } catch (e) {
      const errorMsg = e instanceof Error ? e.message : String(e);
      await this.repo.updateSandboxSpawnError(actor.sessionId, errorMsg);
      await this.repo.updateSandboxStatus(actor.sessionId, "failed");
      this.broadcast(actor, { type: "sandbox_error", error: errorMsg });
      actor.isProcessing = false;
      logger.error("Sandbox spawn failed", { session_id: actor.sessionId, error: errorMsg });
    }
  }

  /**
   * Handle sandbox events (called when sandbox connects back via WebSocket).
   */
  async handleSandboxEvent(sessionId: string, event: SandboxEvent): Promise<void> {
    const actor = this.getActor(sessionId);

    // Store event in database
    await this.repo.createEvent({
      id: generateId(),
      sessionId,
      type: event.type,
      data: JSON.stringify(event),
      messageId: "messageId" in event ? (event as any).messageId : undefined,
    });

    // Handle specific event types
    switch (event.type) {
      case "heartbeat":
        await this.repo.updateSandboxHeartbeat(sessionId);
        break;

      case "git_sync":
        await this.repo.updateSandboxGitSyncStatus(sessionId, event.status);
        if (event.status === "completed") {
          await this.repo.updateSandboxStatus(sessionId, "ready");
          this.broadcast(actor, { type: "sandbox_ready" });
          if (event.sha) {
            await this.repo.updateSessionCurrentSha(sessionId, event.sha);
          }
        }
        break;

      case "execution_complete":
        actor.isProcessing = false;
        this.broadcast(actor, { type: "processing_status", isProcessing: false });

        // Update message status
        if (event.messageId) {
          await this.repo.updateMessageCompletion(
            event.messageId,
            event.success ? "completed" : "failed",
            event.error
          );
        }

        // Process next message if any
        await this.processNextMessage(actor);

        // Update last activity for inactivity timeout
        await this.repo.updateSandboxLastActivity(sessionId);
        break;

      case "push_complete":
        if (event.branchName) {
          await this.repo.updateSessionBranch(sessionId, event.branchName);
        }
        break;
    }

    // Broadcast to all connected clients
    this.broadcast(actor, { type: "sandbox_event", event });
  }

  /**
   * Stop execution in a session.
   */
  async stopExecution(sessionId: string): Promise<void> {
    const actor = this.actors.get(sessionId);
    if (!actor) return;

    actor.isProcessing = false;
    this.broadcast(actor, { type: "processing_status", isProcessing: false });
  }

  /**
   * Generate a WebSocket authentication token for a user.
   */
  async generateWsToken(sessionId: string, body: {
    userId: string;
    githubUserId?: string;
    githubLogin?: string;
    githubName?: string;
    githubEmail?: string;
    githubTokenEncrypted?: string;
    githubRefreshTokenEncrypted?: string;
    githubTokenExpiresAt?: number;
  }): Promise<string> {
    const token = generateId() + generateId(); // Long random token
    const tokenHash = await hashToken(token);

    // Find or create participant
    let participant = await this.repo.getParticipantByUserId(sessionId, body.userId);

    if (participant) {
      // Update existing participant
      await this.repo.updateParticipantWsToken(participant.id as string, tokenHash);
      if (body.githubTokenEncrypted) {
        await this.repo.updateParticipantTokens(
          participant.id as string,
          body.githubTokenEncrypted,
          body.githubRefreshTokenEncrypted,
          body.githubTokenExpiresAt
        );
      }
    } else {
      // Create new participant
      const participantId = generateId();
      await this.repo.createParticipant({
        id: participantId,
        sessionId,
        userId: body.userId,
        githubUserId: body.githubUserId,
        githubLogin: body.githubLogin,
        githubEmail: body.githubEmail,
        githubName: body.githubName,
        role: "member",
        githubAccessTokenEncrypted: body.githubTokenEncrypted,
        githubRefreshTokenEncrypted: body.githubRefreshTokenEncrypted,
        githubTokenExpiresAt: body.githubTokenExpiresAt,
        wsAuthToken: tokenHash,
        wsTokenCreatedAt: Date.now(),
      });
    }

    return token;
  }

  /**
   * Create a pull request from a session.
   */
  async createPR(sessionId: string, body: {
    title: string;
    body: string;
    baseBranch?: string;
    headBranch?: string;
  }): Promise<Record<string, unknown>> {
    // Delegate to the source control provider
    // This would call GitHub API using the participant's OAuth token
    const session = await this.repo.getSession(sessionId);
    if (!session) return { error: "Session not found" };

    const author = await this.repo.getProcessingMessageAuthor(sessionId);

    // For now, return a stub - the actual GitHub PR creation logic
    // from the existing source-control provider can be reused
    return {
      status: "created",
      sessionId,
      title: body.title,
      repoOwner: session.repo_owner,
      repoName: session.repo_name,
      headBranch: body.headBranch || session.branch_name,
      baseBranch: body.baseBranch || session.repo_default_branch,
    };
  }

  /**
   * Handle typing indicator - potentially warm a sandbox.
   */
  private async handleTyping(actor: SessionActor): Promise<void> {
    const sandbox = await this.repo.getSandbox(actor.sessionId);
    if (!sandbox || sandbox.status === "pending") {
      // Pre-warm sandbox on typing
      this.broadcast(actor, { type: "sandbox_warming" });
    }
  }

  /**
   * Handle fetch_history message.
   */
  private async handleFetchHistory(
    actor: SessionActor,
    ws: HonoWebSocket,
    msg: Extract<ClientMessage, { type: "fetch_history" }>
  ): Promise<void> {
    const limit = msg.limit || 100;
    const events = await this.repo.getEventsHistoryPage(
      actor.sessionId,
      msg.cursor.timestamp,
      msg.cursor.id,
      limit + 1 // Fetch one extra to check if there's more
    );

    const hasMore = events.length > limit;
    const items = events.slice(0, limit).map((e: any) => JSON.parse(e.data));
    const cursor = items.length > 0
      ? { timestamp: (events[items.length - 1] as any).created_at, id: (events[items.length - 1] as any).id }
      : null;

    this.sendToWs(ws, { type: "history_page", items, hasMore, cursor });
  }

  /**
   * Broadcast a message to all clients in a session.
   */
  private broadcast(actor: SessionActor, msg: ServerMessage): void {
    const data = JSON.stringify(msg);
    for (const [, client] of actor.clients) {
      try {
        client.ws.send(data);
      } catch {
        // Client disconnected, will be cleaned up on close
      }
    }
  }

  /**
   * Send a message to a specific WebSocket.
   */
  private sendToWs(ws: HonoWebSocket, msg: ServerMessage): void {
    try {
      ws.send(JSON.stringify(msg));
    } catch {
      // WebSocket closed
    }
  }

  /**
   * Start heartbeat monitoring for a sandbox.
   */
  private startHeartbeatMonitor(actor: SessionActor): void {
    if (actor.heartbeatTimer) clearInterval(actor.heartbeatTimer);

    actor.heartbeatTimer = setInterval(async () => {
      const sandbox = await this.repo.getSandbox(actor.sessionId);
      if (!sandbox || !sandbox.last_heartbeat) return;

      const now = Date.now();
      const lastHeartbeat = sandbox.last_heartbeat as number;

      // If no heartbeat for 60 seconds, mark as stale
      if (now - lastHeartbeat > 60000) {
        await this.repo.updateSandboxStatus(actor.sessionId, "stale");
        this.broadcast(actor, { type: "sandbox_status", status: "stale" });
      }
    }, 30000); // Check every 30 seconds
  }

  /**
   * Start inactivity timer for sandbox shutdown.
   */
  private startInactivityTimer(actor: SessionActor): void {
    if (actor.inactivityTimer) clearTimeout(actor.inactivityTimer);

    const timeoutMs = 600000; // 10 minutes

    actor.inactivityTimer = setTimeout(async () => {
      // Take snapshot and shut down sandbox
      const sandbox = await this.repo.getSandbox(actor.sessionId);
      if (!sandbox || !sandbox.provider_object_id) return;

      try {
        const result = await this.sandboxProvider.takeSnapshot?.({
          providerObjectId: sandbox.provider_object_id as string,
          sessionId: actor.sessionId,
          reason: "inactivity_timeout",
        });

        if (result?.success && result.imageId) {
          await this.repo.updateSandboxSnapshotImageId(actor.sessionId, result.imageId);
          this.broadcast(actor, {
            type: "snapshot_saved",
            imageId: result.imageId,
            reason: "inactivity_timeout",
          });
        }
      } catch (e) {
        logger.error("Snapshot failed", { session_id: actor.sessionId, error: e });
      }

      await this.repo.updateSandboxStatus(actor.sessionId, "stopped");
      this.broadcast(actor, { type: "sandbox_status", status: "stopped" });
    }, timeoutMs);
  }
}
