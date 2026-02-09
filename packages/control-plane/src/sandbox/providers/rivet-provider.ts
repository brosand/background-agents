/**
 * Rivet sandbox provider.
 *
 * Creates containers via Rivet's REST API. Each container runs sandbox-agent,
 * which provides an HTTP/SSE API for controlling coding agents (Claude Code,
 * Codex, OpenCode, Amp). The control plane communicates with sandbox-agent
 * via its TypeScript SDK rather than raw WebSockets.
 */

import { generateInternalToken } from "@open-inspect/shared";
import { createLogger } from "../../logger";
import type {
  SandboxProvider,
  SandboxProviderCapabilities,
  CreateSandboxConfig,
  CreateSandboxResult,
} from "../provider";
import { SandboxProviderError } from "../provider";

const logger = createLogger("rivet-provider");

const SANDBOX_AGENT_PORT = 2468;

export interface RivetProviderConfig {
  apiUrl: string;
  token: string;
  project: string;
  environment: string;
  sandboxBuildTag: string;
  internalSecret: string;
}

export class RivetSandboxProvider implements SandboxProvider {
  readonly name = "rivet";
  readonly capabilities: SandboxProviderCapabilities = {
    supportsSnapshots: false,
    supportsRestore: false,
    supportsWarm: false,
  };

  private config: RivetProviderConfig;

  constructor(config: RivetProviderConfig) {
    this.config = config;
  }

  private get headers(): Record<string, string> {
    return {
      Authorization: `Bearer ${this.config.token}`,
      "Content-Type": "application/json",
    };
  }

  async createSandbox(config: CreateSandboxConfig): Promise<CreateSandboxResult> {
    const startTime = Date.now();

    try {
      // Generate an internal auth token for sandbox → control plane communication
      const sandboxCallbackToken = await generateInternalToken(this.config.internalSecret);

      // Generate a token for authenticating to the sandbox-agent HTTP API
      const sandboxAgentToken = crypto.randomUUID();

      const response = await fetch(`${this.config.apiUrl}/actors`, {
        method: "POST",
        headers: this.headers,
        body: JSON.stringify({
          tags: {
            sandboxId: config.sandboxId,
            sessionId: config.sessionId,
            repoOwner: config.repoOwner,
            repoName: config.repoName,
            role: "sandbox",
          },
          buildTags: {
            name: "sandbox",
            current: this.config.sandboxBuildTag,
          },
          network: {
            ports: {
              http: {
                protocol: "https",
                routing: { guard: {} },
                internalPort: SANDBOX_AGENT_PORT,
              },
            },
          },
          resources: {
            cpu: 2000, // 2 CPU cores (millicores)
            memory: 4096, // 4 GB
          },
          environment: {
            // sandbox-agent configuration
            SANDBOX_AGENT_TOKEN: sandboxAgentToken,

            // Session metadata (used by sandbox-agent git setup)
            SANDBOX_ID: config.sandboxId,
            SESSION_ID: config.sessionId,
            REPO_OWNER: config.repoOwner,
            REPO_NAME: config.repoName,

            // LLM configuration (available to coding agents inside the container)
            PROVIDER: config.provider || "anthropic",
            MODEL: config.model || "claude-sonnet-4-5",

            // Git configuration
            ...(config.gitUserName ? { GIT_USER_NAME: config.gitUserName } : {}),
            ...(config.gitUserEmail ? { GIT_USER_EMAIL: config.gitUserEmail } : {}),

            // User-provided env vars (repo secrets, API keys, etc.)
            ...(config.userEnvVars || {}),
          },
          lifecycle: {
            durable: true,
          },
        }),
      });

      if (!response.ok) {
        const text = await response.text();
        throw SandboxProviderError.fromFetchError(
          `Rivet API error: ${response.status} ${text}`,
          new Error(text),
          response.status
        );
      }

      const result = (await response.json()) as {
        actor: {
          id: string;
          createdAt: string;
          network: {
            ports: Record<string, { hostname: string; port: number; path: string; protocol: string }>;
          };
        };
      };

      // Extract the sandbox-agent URL from the Rivet response
      const httpPort = result.actor.network?.ports?.http;
      const sandboxUrl = httpPort
        ? `${httpPort.protocol || "https"}://${httpPort.hostname}:${httpPort.port}${httpPort.path || ""}`
        : null;

      const durationMs = Date.now() - startTime;
      logger.info("Sandbox created via Rivet", {
        event: "rivet.create_sandbox",
        sandbox_id: config.sandboxId,
        session_id: config.sessionId,
        actor_id: result.actor.id,
        sandbox_url: sandboxUrl,
        duration_ms: durationMs,
      });

      return {
        sandboxId: config.sandboxId,
        providerObjectId: result.actor.id,
        status: "spawning",
        createdAt: Date.now(),
        sandboxUrl,
        sandboxAgentToken,
      };
    } catch (err) {
      if (err instanceof SandboxProviderError) throw err;

      throw SandboxProviderError.fromFetchError(
        `Failed to create sandbox via Rivet: ${err instanceof Error ? err.message : String(err)}`,
        err
      );
    }
  }

  /**
   * Destroy a sandbox actor.
   */
  async destroySandbox(actorId: string): Promise<void> {
    try {
      const response = await fetch(`${this.config.apiUrl}/actors/${actorId}`, {
        method: "DELETE",
        headers: this.headers,
      });

      if (!response.ok && response.status !== 404) {
        const text = await response.text();
        logger.error("Failed to destroy sandbox", {
          event: "rivet.destroy_error",
          actor_id: actorId,
          status: response.status,
          body: text,
        });
      } else {
        logger.info("Sandbox destroyed", {
          event: "rivet.destroy_sandbox",
          actor_id: actorId,
        });
      }
    } catch (err) {
      logger.error("Failed to destroy sandbox", {
        event: "rivet.destroy_error",
        actor_id: actorId,
        error: err instanceof Error ? err : String(err),
      });
    }
  }

  /**
   * Get the status of a sandbox actor.
   */
  async getSandboxStatus(
    actorId: string
  ): Promise<{ status: string; createdAt: string } | null> {
    try {
      const response = await fetch(`${this.config.apiUrl}/actors/${actorId}`, {
        method: "GET",
        headers: this.headers,
      });

      if (!response.ok) {
        if (response.status === 404) return null;
        return null;
      }

      const result = (await response.json()) as {
        actor: { id: string; createdAt: string; startedAt?: string; destroyedAt?: string };
      };

      let status = "running";
      if (result.actor.destroyedAt) status = "stopped";
      else if (!result.actor.startedAt) status = "spawning";

      return { status, createdAt: result.actor.createdAt };
    } catch {
      return null;
    }
  }

  /**
   * Health check against Rivet API.
   */
  async healthCheck(): Promise<boolean> {
    try {
      const response = await fetch(`${this.config.apiUrl}/actors`, {
        method: "GET",
        headers: this.headers,
      });
      return response.ok;
    } catch {
      return false;
    }
  }
}

export function createRivetProvider(config: RivetProviderConfig): RivetSandboxProvider {
  return new RivetSandboxProvider(config);
}
