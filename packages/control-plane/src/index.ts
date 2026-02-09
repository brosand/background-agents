/**
 * Open-Inspect Control Plane — Node.js entry point.
 *
 * Replaces the Cloudflare Workers entry point with a Node.js HTTP/WebSocket server
 * using Hono for routing and the `ws` library for WebSocket support.
 */

import { serve } from "@hono/node-server";
import { WebSocketServer } from "ws";
import type { IncomingMessage } from "node:http";
import type { Duplex } from "node:stream";

import { loadConfig } from "./config";
import { getPool, closePool } from "./db/postgres";
import { RedisCache } from "./db/redis";
import { createRivetProvider } from "./sandbox/providers/rivet-provider";
import { SessionManager } from "./session/manager";
import { createRouter } from "./router";
import { createLogger } from "./logger";

const logger = createLogger("server");

async function main() {
  const config = loadConfig();
  logger.info("Starting control plane", {
    port: config.port,
    host: config.host,
    deployment: config.deploymentName,
  });

  // Initialize services
  const pool = getPool(config.databaseUrl);

  const redis = new RedisCache(config.redisUrl);
  await redis.connect();

  const sandboxProvider = createRivetProvider({
    apiUrl: config.rivetApiUrl,
    token: config.rivetToken,
    project: config.rivetProject,
    environment: config.rivetEnvironment,
    sandboxBuildTag: config.rivetSandboxBuildTag,
    internalSecret: config.internalCallbackSecret,
  });

  const sessionManager = new SessionManager(pool, config, sandboxProvider, redis);

  // Create Hono router
  const app = createRouter(pool, redis, sandboxProvider, sessionManager, config);

  // Create HTTP server via Hono
  const server = serve({
    fetch: app.fetch,
    port: config.port,
    hostname: config.host,
  });

  // Create WebSocket server attached to the HTTP server
  const wss = new WebSocketServer({ noServer: true });

  // Handle WebSocket upgrades
  (server as ReturnType<typeof serve>).on(
    "upgrade",
    (request: IncomingMessage, socket: Duplex, head: Buffer) => {
      const url = new URL(request.url || "/", `http://${request.headers.host}`);

      // Client WebSocket: /sessions/:id/ws
      const clientMatch = url.pathname.match(/^\/sessions\/([^/]+)\/ws$/);
      if (clientMatch) {
        const sessionId = clientMatch[1];
        wss.handleUpgrade(request, socket, head, (ws) => {
          logger.info("Client WebSocket connected", { session_id: sessionId });
          sessionManager.handleWebSocketConnection(sessionId, ws);
        });
        return;
      }

      // Sandbox WebSocket: /sessions/:id/sandbox-ws
      const sandboxMatch = url.pathname.match(
        /^\/sessions\/([^/]+)\/sandbox-ws$/
      );
      if (sandboxMatch) {
        const sessionId = sandboxMatch[1];
        wss.handleUpgrade(request, socket, head, (ws) => {
          logger.info("Sandbox WebSocket connected", {
            session_id: sessionId,
          });
          sessionManager.handleSandboxConnection(sessionId, ws);
        });
        return;
      }

      // Unknown WebSocket path
      socket.destroy();
    }
  );

  // Periodic cleanup of expired WS tokens
  const cleanupInterval = setInterval(
    () => {
      sessionManager.cleanupExpiredTokens().catch((err) => {
        logger.error("Token cleanup failed", {
          error: err instanceof Error ? err : String(err),
        });
      });
    },
    5 * 60 * 1000
  ); // Every 5 minutes

  // Graceful shutdown
  const shutdown = async (signal: string) => {
    logger.info(`Received ${signal}, shutting down...`);
    clearInterval(cleanupInterval);

    await sessionManager.shutdown();

    wss.close();
    (server as ReturnType<typeof serve>).close();

    await redis.disconnect();
    await closePool();

    logger.info("Shutdown complete");
    process.exit(0);
  };

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));

  logger.info(`Control plane listening on ${config.host}:${config.port}`);
}

main().catch((err) => {
  logger.error("Fatal startup error", {
    error: err instanceof Error ? err : String(err),
  });
  process.exit(1);
});
