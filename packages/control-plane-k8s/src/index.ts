/**
 * Main entry point — Node.js HTTP + WebSocket server.
 *
 * Replaces Cloudflare Workers entry point (packages/control-plane/src/index.ts).
 *
 * Architecture:
 * - Hono handles HTTP requests
 * - Node.js http.Server handles WebSocket upgrades
 * - SessionActorRegistry routes WebSocket connections to the correct actor
 * - PostgreSQL replaces D1 + DO SQLite
 * - Redis replaces KV Namespace
 */

import { createServer, type IncomingMessage } from "node:http";
import { WebSocketServer, WebSocket } from "ws";
import { serve } from "@hono/node-server";
import { getPool, closePool } from "./db/pool.js";
import { getRedis, closeRedis } from "./db/cache.js";
import { migrate } from "./db/migrate.js";
import { SessionActorRegistry } from "./session/registry.js";
import { createApp } from "./server.js";
import { loadConfig } from "./utils/config.js";
import { createLogger } from "./utils/logger.js";
import { SessionIndexStore } from "./db/session-index.js";

const log = createLogger("main");
const config = loadConfig();

async function main(): Promise<void> {
  log.info("Starting control plane", {
    port: config.port,
    host: config.host,
    deployment: config.deploymentName,
  });

  // Run migrations
  await migrate();

  // Initialize dependencies
  const pool = getPool();
  const redis = getRedis();
  await redis.ping();

  const registry = new SessionActorRegistry(pool, config);

  // Create Hono app
  const app = createApp({ pool, redis, registry, config });

  // Create Node.js HTTP server from Hono
  const server = createServer(async (req, res) => {
    // Delegate to Hono for non-upgrade HTTP requests
    try {
      const url = new URL(req.url!, `http://${req.headers.host}`);
      const honoReq = new Request(url.toString(), {
        method: req.method,
        headers: Object.fromEntries(
          Object.entries(req.headers)
            .filter(([, v]) => v !== undefined)
            .map(([k, v]) => [k, Array.isArray(v) ? v[0] : v!])
        ),
        body: req.method !== "GET" && req.method !== "HEAD"
          ? await readBody(req)
          : undefined,
      });

      const honoRes = await app.fetch(honoReq);

      res.writeHead(honoRes.status, Object.fromEntries(honoRes.headers.entries()));
      const body = await honoRes.arrayBuffer();
      res.end(Buffer.from(body));
    } catch (err) {
      log.error("Request handling error", { error: err instanceof Error ? err : String(err) });
      res.writeHead(500);
      res.end("Internal Server Error");
    }
  });

  // WebSocket server for session connections
  const wss = new WebSocketServer({ noServer: true });

  server.on("upgrade", async (request, socket, head) => {
    const url = new URL(request.url!, `http://${request.headers.host}`);

    // Match /sessions/:id/ws
    const match = url.pathname.match(/^\/sessions\/([^/]+)\/ws$/);
    if (!match) {
      socket.destroy();
      return;
    }

    const sessionId = match[1];
    const isSandbox = url.searchParams.get("type") === "sandbox";
    const sandboxId = url.searchParams.get("sandbox_id") ?? undefined;

    // Verify session exists
    const store = new SessionIndexStore(pool);
    const session = await store.get(sessionId);
    if (!session) {
      socket.write("HTTP/1.1 404 Not Found\r\n\r\n");
      socket.destroy();
      return;
    }

    wss.handleUpgrade(request, socket, head, (ws) => {
      const actor = registry.getOrCreate(sessionId);
      actor.handleWebSocketUpgrade(ws, isSandbox, sandboxId).catch((err) => {
        log.error("WebSocket upgrade handler error", {
          session_id: sessionId,
          error: err instanceof Error ? err : String(err),
        });
        ws.close(1011, "Internal error");
      });
    });
  });

  // Start server
  server.listen(config.port, config.host, () => {
    log.info("Server listening", { port: config.port, host: config.host });
  });

  // Graceful shutdown
  const shutdown = async (signal: string) => {
    log.info("Received shutdown signal", { signal });
    registry.destroyAll();

    server.close();

    await closePool();
    await closeRedis();

    log.info("Shutdown complete");
    process.exit(0);
  };

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
}

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => resolve(Buffer.concat(chunks).toString()));
    req.on("error", reject);
  });
}

main().catch((err) => {
  log.error("Fatal error", { error: err instanceof Error ? err : String(err) });
  process.exit(1);
});
