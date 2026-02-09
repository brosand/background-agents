/**
 * SessionActor — Rivet Actor replacement for Cloudflare Durable Object (SessionDO).
 *
 * Architecture changes from Cloudflare:
 * - Long-lived process (no hibernation/wake cycles)
 * - PostgreSQL for state (replaces DO SQLite)
 * - Standard `ws` library (replaces Cloudflare WebSocketPair + hibernation tags)
 * - setTimeout/setInterval (replaces DO Alarms)
 * - Plain async calls (replaces ctx.waitUntil())
 *
 * The actor is addressed by session ID. Rivet guarantees single-instance
 * per ID, just like Cloudflare Durable Objects.
 *
 * This is a structural port — the business logic (message queue, event
 * processing, sandbox lifecycle) is preserved from the original.
 */

import type { WebSocket as WsWebSocket } from "ws";
import type pg from "pg";
import { SessionRepository } from "../db/session-repository.js";
import { WebSocketManager, type ClientInfo } from "./websocket-manager.js";
import { generateId, hashToken } from "../auth/crypto.js";
import { createLogger, type Logger } from "../utils/logger.js";
import type { Config } from "../utils/config.js";

// Event types from the sandbox
interface SandboxEvent {
  type: string;
  messageId?: string;
  [key: string]: unknown;
}

// Messages from web clients
interface ClientMessage {
  type: string;
  [key: string]: unknown;
}

interface ServerMessage {
  type: string;
  [key: string]: unknown;
}

interface SessionState {
  id: string;
  title: string | null;
  repoOwner: string;
  repoName: string;
  branchName: string | null;
  status: string;
  sandboxStatus: string;
  messageCount: number;
  createdAt: number;
  model: string;
  reasoningEffort?: string;
  isProcessing: boolean;
}

function getGitHubAvatarUrl(login: string | null): string | null {
  return login ? `https://github.com/${login}.png` : null;
}

export class SessionActor {
  private readonly repository: SessionRepository;
  private readonly wsManager: WebSocketManager;
  private readonly log: Logger;

  private inactivityTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    private readonly sessionId: string,
    private readonly pool: pg.Pool,
    private readonly config: Config
  ) {
    this.repository = new SessionRepository(pool, sessionId);
    this.wsManager = new WebSocketManager(sessionId, this.repository);
    this.log = createLogger("session-actor", { session_id: sessionId });
  }

  // === WebSocket upgrade handling ===

  async handleWebSocketUpgrade(ws: WsWebSocket, isSandbox: boolean, sandboxId?: string): Promise<void> {
    if (isSandbox) {
      this.wsManager.acceptSandboxSocket(ws);
      this.log.info("Sandbox WebSocket connected", { sandbox_id: sandboxId });

      // Process any pending messages now that sandbox is connected
      await this.processMessageQueue();
    } else {
      const wsId = `ws-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
      this.wsManager.acceptClientSocket(ws, wsId);

      // Enforce auth timeout in background
      this.wsManager.enforceAuthTimeout(ws, wsId).catch(() => {});
    }

    // Set up message handling
    ws.on("message", async (data) => {
      const message = data.toString();
      const { kind } = this.wsManager.classify(ws);
      if (kind === "sandbox") {
        await this.handleSandboxMessage(message);
      } else {
        await this.handleClientMessage(ws, message);
      }
    });

    ws.on("close", async () => {
      const { kind } = this.wsManager.classify(ws);
      if (kind === "sandbox") {
        this.wsManager.clearSandboxSocket();
        await this.repository.updateSandboxStatus("stopped");
      } else {
        const client = this.wsManager.removeClient(ws);
        if (client) {
          this.broadcast({ type: "presence_leave", userId: client.userId });
        }
      }
    });

    ws.on("error", (error) => {
      this.log.error("WebSocket error", { error: error instanceof Error ? error : String(error) });
      ws.close(1011, "Internal error");
    });
  }

  // === HTTP handlers (forwarded from the API server) ===

  async handleInit(body: {
    sessionName: string;
    repoOwner: string;
    repoName: string;
    repoId?: number;
    title?: string;
    model?: string;
    reasoningEffort?: string;
    userId: string;
    githubLogin?: string;
    githubName?: string;
    githubEmail?: string;
    githubToken?: string | null;
    githubTokenEncrypted?: string | null;
  }): Promise<{ sessionId: string; status: string }> {
    const now = Date.now();
    const model = body.model || "claude-haiku-4-5";

    // Create session in both the index and state tables
    await this.repository.upsertSession({
      sessionName: body.sessionName,
      title: body.title ?? null,
      repoOwner: body.repoOwner,
      repoName: body.repoName,
      repoId: body.repoId ?? null,
      model,
      reasoningEffort: body.reasoningEffort ?? null,
      status: "created",
      createdAt: now,
      updatedAt: now,
    });

    // Create sandbox record
    const sandboxId = generateId();
    await this.repository.createSandbox({
      id: sandboxId,
      status: "pending",
      gitSyncStatus: "pending",
      createdAt: 0,
    });

    // Create owner participant
    const participantId = generateId();
    await this.repository.createParticipant({
      id: participantId,
      userId: body.userId,
      githubLogin: body.githubLogin ?? null,
      githubName: body.githubName ?? null,
      githubEmail: body.githubEmail ?? null,
      githubAccessTokenEncrypted: body.githubTokenEncrypted ?? null,
      role: "owner",
      joinedAt: now,
    });

    this.log.info("Session initialized", { session_id: this.sessionId });

    return { sessionId: this.sessionId, status: "created" };
  }

  async handleGetState(): Promise<SessionState | null> {
    const session = await this.repository.getSession();
    if (!session) return null;

    const sandbox = await this.repository.getSandbox();
    const messageCount = await this.repository.getMessageCount();
    const isProcessing = (await this.repository.getProcessingMessage()) !== null;

    return {
      id: session.session_id,
      title: session.title,
      repoOwner: session.repo_owner,
      repoName: session.repo_name,
      branchName: session.branch_name,
      status: session.status,
      sandboxStatus: sandbox?.status ?? "pending",
      messageCount,
      createdAt: Number(session.created_at),
      model: session.model ?? "claude-haiku-4-5",
      reasoningEffort: session.reasoning_effort ?? undefined,
      isProcessing,
    };
  }

  async handleEnqueuePrompt(body: {
    content: string;
    authorId: string;
    source: string;
    model?: string;
    reasoningEffort?: string;
    attachments?: Array<{ type: string; name: string; url?: string }>;
    callbackContext?: unknown;
  }): Promise<{ messageId: string; status: string }> {
    let participant = await this.repository.getParticipantByUserId(body.authorId);
    if (!participant) {
      const id = generateId();
      await this.repository.createParticipant({
        id,
        userId: body.authorId,
        githubName: body.authorId,
        role: "member",
        joinedAt: Date.now(),
      });
      participant = await this.repository.getParticipantByUserId(body.authorId);
    }

    const messageId = generateId();
    const now = Date.now();

    await this.repository.createMessage({
      id: messageId,
      authorId: participant!.id,
      content: body.content,
      source: body.source,
      model: body.model ?? null,
      reasoningEffort: body.reasoningEffort ?? null,
      attachments: body.attachments ? JSON.stringify(body.attachments) : null,
      callbackContext: body.callbackContext ? JSON.stringify(body.callbackContext) : null,
      status: "pending",
      createdAt: now,
    });

    // Write user_message event
    const userMessageEvent: SandboxEvent = {
      type: "user_message",
      content: body.content,
      messageId,
      timestamp: now / 1000,
      author: {
        participantId: participant!.id,
        name: participant!.github_name || participant!.github_login || participant!.user_id,
        avatar: getGitHubAvatarUrl(participant!.github_login),
      },
    };
    await this.repository.createEvent({
      id: generateId(),
      type: "user_message",
      data: JSON.stringify(userMessageEvent),
      messageId,
      createdAt: now,
    });
    this.broadcast({ type: "sandbox_event", event: userMessageEvent });

    this.log.info("prompt.enqueue", {
      event: "prompt.enqueue",
      message_id: messageId,
      source: body.source,
      author_id: participant!.id,
      content_length: body.content.length,
    });

    await this.processMessageQueue();

    return { messageId, status: "queued" };
  }

  async handleStop(): Promise<void> {
    const now = Date.now();
    const processingMessage = await this.repository.getProcessingMessage();

    if (processingMessage) {
      await this.repository.updateMessageCompletion(processingMessage.id, "failed", now);

      this.broadcast({
        type: "sandbox_event",
        event: {
          type: "execution_complete",
          messageId: processingMessage.id,
          success: false,
          sandboxId: "",
          timestamp: now / 1000,
        },
      });
    }

    this.broadcast({ type: "processing_status", isProcessing: false });

    const sandboxWs = this.wsManager.getSandboxSocket();
    if (sandboxWs) {
      this.wsManager.send(sandboxWs, { type: "stop" });
    }
  }

  async handleGenerateWsToken(body: {
    userId: string;
    githubUserId?: string;
    githubLogin?: string;
    githubName?: string;
    githubEmail?: string;
    githubTokenEncrypted?: string | null;
    githubRefreshTokenEncrypted?: string | null;
    githubTokenExpiresAt?: number | null;
  }): Promise<{ token: string; participantId: string }> {
    const now = Date.now();
    let participant = await this.repository.getParticipantByUserId(body.userId);

    if (!participant) {
      const id = generateId();
      await this.repository.createParticipant({
        id,
        userId: body.userId,
        githubUserId: body.githubUserId ?? null,
        githubLogin: body.githubLogin ?? null,
        githubName: body.githubName ?? null,
        githubEmail: body.githubEmail ?? null,
        githubAccessTokenEncrypted: body.githubTokenEncrypted ?? null,
        githubRefreshTokenEncrypted: body.githubRefreshTokenEncrypted ?? null,
        githubTokenExpiresAt: body.githubTokenExpiresAt ?? null,
        role: "member",
        joinedAt: now,
      });
      participant = await this.repository.getParticipantByUserId(body.userId);
    }

    const plainToken = generateId(32);
    const tokenHash = await hashToken(plainToken);
    await this.repository.updateParticipantWsToken(participant!.id, tokenHash, now);

    return { token: plainToken, participantId: participant!.id };
  }

  async handleListEvents(options: {
    cursor?: string;
    limit?: number;
    type?: string;
    messageId?: string;
  }): Promise<{ events: unknown[]; cursor?: string; hasMore: boolean }> {
    const limit = Math.min(options.limit ?? 50, 200);
    // Simplified — full implementation would mirror the CF version's cursor logic
    const events = await this.repository.getEventsForReplay(limit);

    return {
      events: events.map((e) => ({
        id: e.id,
        type: e.type,
        data: typeof e.data === "string" ? JSON.parse(e.data) : e.data,
        messageId: e.message_id,
        createdAt: Number(e.created_at),
      })),
      hasMore: false,
    };
  }

  async handleArchive(userId: string): Promise<{ status: string } | { error: string }> {
    const participant = await this.repository.getParticipantByUserId(userId);
    if (!participant) return { error: "Not authorized to archive this session" };

    const now = Date.now();
    await this.repository.updateSessionStatus("archived", now);
    this.broadcast({ type: "session_status", status: "archived" });
    return { status: "archived" };
  }

  // === Internal message handling ===

  private async handleSandboxMessage(raw: string): Promise<void> {
    try {
      const event = JSON.parse(raw) as SandboxEvent;
      await this.processSandboxEvent(event);
    } catch (e) {
      this.log.error("Error processing sandbox message", {
        error: e instanceof Error ? e : String(e),
      });
    }
  }

  private async handleClientMessage(ws: WsWebSocket, raw: string): Promise<void> {
    try {
      const data = JSON.parse(raw) as ClientMessage;

      switch (data.type) {
        case "ping":
          this.wsManager.send(ws, { type: "pong", timestamp: Date.now() });
          break;

        case "subscribe":
          await this.handleSubscribe(ws, data as { token: string; clientId: string; type: string });
          break;

        case "prompt":
          await this.handlePromptFromClient(ws, data);
          break;

        case "stop":
          await this.handleStop();
          break;

        case "typing":
          // Warm sandbox on typing indicator
          break;

        case "fetch_history":
          await this.handleFetchHistory(ws, data);
          break;

        case "presence":
          this.updatePresence(ws, data as { status: "active" | "idle"; type: string });
          break;
      }
    } catch (e) {
      this.log.error("Error processing client message", {
        error: e instanceof Error ? e : String(e),
      });
      this.wsManager.send(ws, {
        type: "error",
        code: "INVALID_MESSAGE",
        message: "Failed to process message",
      });
    }
  }

  private async handleSubscribe(
    ws: WsWebSocket,
    data: { token: string; clientId: string }
  ): Promise<void> {
    if (!data.token) {
      ws.close(4001, "Authentication required");
      return;
    }

    const tokenHash = await hashToken(data.token);
    const participant = await this.repository.getParticipantByWsTokenHash(tokenHash);

    if (!participant) {
      ws.close(4001, "Invalid authentication token");
      return;
    }

    const clientInfo: ClientInfo = {
      participantId: participant.id,
      userId: participant.user_id,
      name: participant.github_name || participant.github_login || participant.user_id,
      avatar: getGitHubAvatarUrl(participant.github_login),
      status: "active",
      lastSeen: Date.now(),
      clientId: data.clientId,
      ws,
    };

    this.wsManager.setClient(ws, clientInfo);

    const state = await this.handleGetState();
    this.wsManager.send(ws, {
      type: "subscribed",
      sessionId: state?.id,
      state,
      participantId: participant.id,
      participant: {
        participantId: participant.id,
        name: clientInfo.name,
        avatar: clientInfo.avatar,
      },
    });

    // Send historical events
    const REPLAY_LIMIT = 500;
    const events = await this.repository.getEventsForReplay(REPLAY_LIMIT);
    for (const event of events) {
      try {
        const eventData = typeof event.data === "string" ? JSON.parse(event.data) : event.data;
        this.wsManager.send(ws, { type: "sandbox_event", event: eventData });
      } catch {
        // Skip malformed events
      }
    }

    this.wsManager.send(ws, {
      type: "replay_complete",
      hasMore: events.length >= REPLAY_LIMIT,
      cursor: events.length > 0
        ? { timestamp: Number(events[0].created_at), id: events[0].id }
        : null,
    });

    // Send presence
    const participants = this.wsManager.getAuthenticatedClients().map((c) => ({
      participantId: c.participantId,
      userId: c.userId,
      name: c.name,
      avatar: c.avatar,
      status: c.status,
      lastSeen: c.lastSeen,
    }));
    this.wsManager.send(ws, { type: "presence_sync", participants });
    this.broadcast({ type: "presence_update", participants });
  }

  private async handlePromptFromClient(ws: WsWebSocket, data: ClientMessage): Promise<void> {
    const client = this.wsManager.getClient(ws);
    if (!client) {
      this.wsManager.send(ws, { type: "error", code: "NOT_SUBSCRIBED", message: "Must subscribe first" });
      return;
    }

    const result = await this.handleEnqueuePrompt({
      content: data.content as string,
      authorId: client.userId,
      source: "web",
      model: data.model as string | undefined,
      reasoningEffort: data.reasoningEffort as string | undefined,
    });

    this.wsManager.send(ws, {
      type: "prompt_queued",
      messageId: result.messageId,
      position: await this.repository.getPendingOrProcessingCount(),
    });
  }

  private async handleFetchHistory(ws: WsWebSocket, data: ClientMessage): Promise<void> {
    const client = this.wsManager.getClient(ws);
    if (!client) {
      this.wsManager.send(ws, { type: "error", code: "NOT_SUBSCRIBED", message: "Must subscribe first" });
      return;
    }

    const cursor = data.cursor as { timestamp: number; id: string } | undefined;
    if (!cursor) {
      this.wsManager.send(ws, { type: "error", code: "INVALID_CURSOR", message: "Invalid cursor" });
      return;
    }

    const limit = Math.max(1, Math.min((data.limit as number) || 200, 500));
    const page = await this.repository.getEventsHistoryPage(cursor.timestamp, cursor.id, limit);

    const items: SandboxEvent[] = [];
    for (const event of page.events) {
      try {
        items.push(typeof event.data === "string" ? JSON.parse(event.data) : event.data);
      } catch {
        // Skip
      }
    }

    const oldestEvent = page.events.length > 0 ? page.events[0] : null;
    this.wsManager.send(ws, {
      type: "history_page",
      items,
      hasMore: page.hasMore,
      cursor: oldestEvent ? { timestamp: Number(oldestEvent.created_at), id: oldestEvent.id } : null,
    });
  }

  private updatePresence(ws: WsWebSocket, data: { status: "active" | "idle" }): void {
    const client = this.wsManager.getClient(ws);
    if (client) {
      client.status = data.status;
      client.lastSeen = Date.now();
      const participants = this.wsManager.getAuthenticatedClients().map((c) => ({
        participantId: c.participantId,
        userId: c.userId,
        name: c.name,
        avatar: c.avatar,
        status: c.status,
        lastSeen: c.lastSeen,
      }));
      this.broadcast({ type: "presence_update", participants });
    }
  }

  // === Sandbox event processing ===

  private async processSandboxEvent(event: SandboxEvent): Promise<void> {
    const now = Date.now();

    if (event.type === "heartbeat") {
      await this.repository.updateSandboxHeartbeat(now);
      return;
    }

    const eventId = generateId();
    const processingMessage = await this.repository.getProcessingMessage();
    const messageId = event.messageId ?? processingMessage?.id ?? null;

    await this.repository.createEvent({
      id: eventId,
      type: event.type,
      data: JSON.stringify(event),
      messageId,
      createdAt: now,
    });

    if (event.type === "execution_complete") {
      const completionMessageId = messageId;
      const isStillProcessing = completionMessageId != null && processingMessage?.id === completionMessageId;

      if (isStillProcessing) {
        const execEvent = event as { success: boolean };
        const status = execEvent.success ? "completed" : "failed";
        await this.repository.updateMessageCompletion(completionMessageId, status, now);

        this.broadcast({ type: "sandbox_event", event });
        this.broadcast({ type: "processing_status", isProcessing: false });
      }

      await this.repository.updateSandboxLastActivity(now);
      this.scheduleInactivityCheck();
      await this.processMessageQueue();
      return;
    }

    if (event.type === "git_sync") {
      const syncEvent = event as { status: string; sha?: string };
      await this.repository.updateSandboxGitSyncStatus(syncEvent.status);
      if (syncEvent.sha) {
        await this.repository.updateSessionCurrentSha(syncEvent.sha);
      }
    }

    this.broadcast({ type: "sandbox_event", event });
  }

  // === Message queue processing ===

  private async processMessageQueue(): Promise<void> {
    if (await this.repository.getProcessingMessage()) return;

    const message = await this.repository.getNextPendingMessage();
    if (!message) return;

    const sandboxWs = this.wsManager.getSandboxSocket();
    if (!sandboxWs) {
      this.log.info("prompt.dispatch", {
        event: "prompt.dispatch",
        message_id: message.id,
        outcome: "deferred",
        reason: "no_sandbox",
      });
      this.broadcast({ type: "sandbox_spawning" });
      // TODO: Trigger sandbox spawn via Modal API
      return;
    }

    const now = Date.now();
    await this.repository.updateMessageToProcessing(message.id, now);
    this.broadcast({ type: "processing_status", isProcessing: true });

    await this.repository.updateSandboxLastActivity(now);

    const author = await this.repository.getParticipantById(message.author_id);
    const session = await this.repository.getSession();
    const resolvedModel = message.model || session?.model || "claude-haiku-4-5";

    const command = {
      type: "prompt",
      messageId: message.id,
      content: message.content,
      model: resolvedModel,
      author: {
        userId: author?.user_id ?? "unknown",
        githubName: author?.github_name ?? null,
        githubEmail: author?.github_email ?? null,
      },
    };

    this.wsManager.send(sandboxWs, command);

    this.log.info("prompt.dispatch", {
      event: "prompt.dispatch",
      message_id: message.id,
      outcome: "sent",
      model: resolvedModel,
    });
  }

  // === Inactivity check (replaces DO Alarms) ===

  private scheduleInactivityCheck(): void {
    if (this.inactivityTimer) {
      clearTimeout(this.inactivityTimer);
    }

    this.inactivityTimer = setTimeout(async () => {
      const sandbox = await this.repository.getSandbox();
      if (!sandbox || !sandbox.last_activity) return;

      const idleMs = Date.now() - Number(sandbox.last_activity);
      if (idleMs >= this.config.sandboxInactivityTimeoutMs) {
        this.log.info("Sandbox inactivity timeout", { idle_ms: idleMs });
        // TODO: Trigger snapshot + sandbox shutdown
      }
    }, this.config.sandboxInactivityTimeoutMs);
  }

  // === Broadcast ===

  private broadcast(message: ServerMessage): void {
    this.wsManager.forEachClientSocket("authenticated_only", (ws) => {
      this.wsManager.send(ws, message);
    });
  }

  // === Cleanup ===

  destroy(): void {
    if (this.inactivityTimer) {
      clearTimeout(this.inactivityTimer);
    }
    this.wsManager.destroy();
  }
}
