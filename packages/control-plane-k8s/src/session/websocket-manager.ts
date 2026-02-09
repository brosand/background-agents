/**
 * WebSocketManager — replaces Cloudflare DO WebSocket primitives with the `ws` library.
 *
 * Key differences from the Cloudflare version:
 * - No WebSocketPair — we use the `ws` library directly
 * - No hibernation tags — we use an in-memory Map (state is persistent because
 *   Rivet actors are long-lived)
 * - No ctx.acceptWebSocket — we manage ws lifecycle ourselves
 * - No ctx.setWebSocketAutoResponse — we handle ping/pong directly
 *
 * The actor process is long-lived, so we don't need hibernation recovery.
 * If the actor crashes, Rivet restarts it and clients reconnect.
 */

import type { WebSocket as WsWebSocket } from "ws";
import type { SessionRepository } from "../db/session-repository.js";
import { createLogger, type Logger } from "../utils/logger.js";

export interface ClientInfo {
  participantId: string;
  userId: string;
  name: string;
  avatar: string | null;
  status: "active" | "idle";
  lastSeen: number;
  clientId: string;
  ws: WsWebSocket;
  lastFetchHistoryAt?: number;
}

interface ParsedSocket {
  kind: "sandbox" | "client";
  wsId?: string;
}

export class WebSocketManager {
  private sandboxWs: WsWebSocket | null = null;
  private readonly clients = new Map<WsWebSocket, ClientInfo>();
  private readonly wsIds = new Map<WsWebSocket, string>();
  private readonly log: Logger;
  private pingInterval: ReturnType<typeof setInterval> | null = null;

  constructor(
    private readonly sessionId: string,
    private readonly repository: SessionRepository
  ) {
    this.log = createLogger("ws-manager", { session_id: sessionId });
  }

  // === Socket acceptance ===

  acceptSandboxSocket(ws: WsWebSocket): void {
    this.sandboxWs = ws;
    this.setupPingPong(ws);
    this.log.info("Sandbox socket accepted");
  }

  acceptClientSocket(ws: WsWebSocket, wsId: string): void {
    this.wsIds.set(ws, wsId);
    this.setupPingPong(ws);
    this.log.debug("Client socket accepted", { ws_id: wsId });
  }

  // === Socket classification ===

  classify(ws: WsWebSocket): ParsedSocket {
    if (ws === this.sandboxWs) {
      return { kind: "sandbox" };
    }
    const wsId = this.wsIds.get(ws);
    return { kind: "client", wsId };
  }

  // === Sandbox socket ===

  getSandboxSocket(): WsWebSocket | null {
    if (this.sandboxWs && this.sandboxWs.readyState === 1 /* OPEN */) {
      return this.sandboxWs;
    }
    return null;
  }

  clearSandboxSocket(): void {
    this.sandboxWs = null;
  }

  // === Client management ===

  setClient(ws: WsWebSocket, info: ClientInfo): void {
    this.clients.set(ws, info);
  }

  getClient(ws: WsWebSocket): ClientInfo | undefined {
    return this.clients.get(ws);
  }

  removeClient(ws: WsWebSocket): ClientInfo | undefined {
    const info = this.clients.get(ws);
    this.clients.delete(ws);
    this.wsIds.delete(ws);
    return info;
  }

  getAuthenticatedClients(): ClientInfo[] {
    return Array.from(this.clients.values());
  }

  forEachClientSocket(
    _filter: "authenticated_only",
    fn: (ws: WsWebSocket) => void
  ): void {
    for (const [ws, _info] of this.clients) {
      if (ws.readyState === 1 /* OPEN */) {
        fn(ws);
      }
    }
  }

  // === Messaging ===

  send(ws: WsWebSocket, data: unknown): boolean {
    try {
      if (ws.readyState === 1 /* OPEN */) {
        ws.send(JSON.stringify(data));
        return true;
      }
    } catch (e) {
      this.log.error("Failed to send WebSocket message", {
        error: e instanceof Error ? e : String(e),
      });
    }
    return false;
  }

  close(ws: WsWebSocket, code: number, reason: string): void {
    try {
      ws.close(code, reason);
    } catch {
      // Socket may already be closed
    }
  }

  // === WS Client Mapping (persisted for reconnection) ===

  async persistClientMapping(wsId: string, participantId: string, clientId: string): Promise<void> {
    await this.repository.upsertWsClientMapping({
      wsId,
      participantId,
      clientId,
      createdAt: Date.now(),
    });
  }

  async recoverClientMapping(ws: WsWebSocket): Promise<{
    participant_id: string;
    client_id: string;
    user_id: string;
    github_name: string | null;
    github_login: string | null;
  } | null> {
    const wsId = this.wsIds.get(ws);
    if (!wsId) return null;
    return this.repository.getWsClientMapping(wsId);
  }

  // === Auth timeout ===

  async enforceAuthTimeout(ws: WsWebSocket, wsId: string): Promise<void> {
    const AUTH_TIMEOUT_MS = 10_000;
    await new Promise((resolve) => setTimeout(resolve, AUTH_TIMEOUT_MS));

    // If still not authenticated (not in clients map), close
    if (!this.clients.has(ws) && ws.readyState === 1 /* OPEN */) {
      this.log.warn("WebSocket auth timeout", { ws_id: wsId });
      ws.close(4001, "Authentication timeout");
    }
  }

  // === Ping/Pong ===

  private setupPingPong(ws: WsWebSocket): void {
    // Send ping every 30 seconds
    const interval = setInterval(() => {
      if (ws.readyState === 1 /* OPEN */) {
        ws.ping();
      } else {
        clearInterval(interval);
      }
    }, 30_000);

    ws.on("close", () => clearInterval(interval));
  }

  // === Cleanup ===

  destroy(): void {
    if (this.pingInterval) {
      clearInterval(this.pingInterval);
    }

    // Close all client sockets
    for (const [ws] of this.clients) {
      this.close(ws, 1001, "Server shutting down");
    }
    this.clients.clear();
    this.wsIds.clear();

    // Close sandbox socket
    if (this.sandboxWs) {
      this.close(this.sandboxWs, 1001, "Server shutting down");
      this.sandboxWs = null;
    }
  }
}
