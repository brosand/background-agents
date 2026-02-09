/**
 * Kubernetes sandbox provider implementation.
 *
 * Replaces ModalSandboxProvider. Calls the sandbox-api service which manages
 * K8s pods instead of Modal sandboxes.
 */

import { generateInternalToken } from "@open-inspect/shared";
import {
  DEFAULT_SANDBOX_TIMEOUT_SECONDS,
  SandboxProviderError,
  type SandboxProvider,
  type SandboxProviderCapabilities,
  type CreateSandboxConfig,
  type CreateSandboxResult,
  type RestoreConfig,
  type RestoreResult,
  type SnapshotConfig,
  type SnapshotResult,
} from "../provider";

/**
 * Kubernetes sandbox provider.
 *
 * Implements the SandboxProvider interface using the sandbox-api service
 * which manages K8s pods for sandbox environments.
 */
export class K8sSandboxProvider implements SandboxProvider {
  readonly name = "kubernetes";

  readonly capabilities: SandboxProviderCapabilities = {
    supportsSnapshots: true,
    supportsRestore: true,
    supportsWarm: true,
  };

  constructor(
    private readonly sandboxApiUrl: string,
    private readonly secret: string
  ) {}

  /**
   * Create a new sandbox pod via the sandbox-api service.
   */
  async createSandbox(config: CreateSandboxConfig): Promise<CreateSandboxResult> {
    try {
      const authToken = await generateInternalToken(this.secret);

      const headers: Record<string, string> = {
        "Content-Type": "application/json",
        Authorization: `Bearer ${authToken}`,
      };
      if (config.traceId) headers["x-trace-id"] = config.traceId;
      if (config.requestId) headers["x-request-id"] = config.requestId;

      const response = await fetch(`${this.sandboxApiUrl}/api/create-sandbox`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          session_id: config.sessionId,
          sandbox_id: config.sandboxId,
          repo_owner: config.repoOwner,
          repo_name: config.repoName,
          control_plane_url: config.controlPlaneUrl,
          sandbox_auth_token: config.sandboxAuthToken,
          opencode_session_id: config.opencodeSessionId || null,
          git_user_name: config.gitUserName || null,
          git_user_email: config.gitUserEmail || null,
          provider: config.provider || "anthropic",
          model: config.model || "claude-sonnet-4-5",
          user_env_vars: config.userEnvVars || null,
        }),
      });

      if (!response.ok) {
        throw this.classifyErrorWithStatus(
          `Create sandbox failed with HTTP ${response.status}`,
          response.status
        );
      }

      const result = (await response.json()) as {
        success: boolean;
        data?: {
          sandbox_id: string;
          pod_name?: string;
          status: string;
          created_at: number;
        };
        error?: string;
      };

      if (!result.success || !result.data) {
        throw new SandboxProviderError(
          `Create sandbox failed: ${result.error || "Unknown error"}`,
          "permanent"
        );
      }

      return {
        sandboxId: result.data.sandbox_id,
        providerObjectId: result.data.pod_name, // K8s pod name as provider ID
        status: result.data.status,
        createdAt: result.data.created_at,
      };
    } catch (error) {
      if (error instanceof SandboxProviderError) throw error;
      throw this.classifyError("Failed to create sandbox", error);
    }
  }

  /**
   * Restore a sandbox from a VolumeSnapshot.
   */
  async restoreFromSnapshot(config: RestoreConfig): Promise<RestoreResult> {
    try {
      const authToken = await generateInternalToken(this.secret);

      const headers: Record<string, string> = {
        "Content-Type": "application/json",
        Authorization: `Bearer ${authToken}`,
      };
      if (config.traceId) headers["x-trace-id"] = config.traceId;
      if (config.requestId) headers["x-request-id"] = config.requestId;

      const response = await fetch(`${this.sandboxApiUrl}/api/restore-sandbox`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          snapshot_image_id: config.snapshotImageId,
          session_config: {
            session_id: config.sessionId,
            repo_owner: config.repoOwner,
            repo_name: config.repoName,
            provider: config.provider,
            model: config.model,
          },
          sandbox_id: config.sandboxId,
          control_plane_url: config.controlPlaneUrl,
          sandbox_auth_token: config.sandboxAuthToken,
          user_env_vars: config.userEnvVars || null,
          timeout_seconds: config.timeoutSeconds ?? DEFAULT_SANDBOX_TIMEOUT_SECONDS,
        }),
      });

      if (!response.ok) {
        throw this.classifyErrorWithStatus(
          `Restore failed with HTTP ${response.status}`,
          response.status
        );
      }

      const result = (await response.json()) as {
        success: boolean;
        data?: { sandbox_id: string; pod_name?: string };
        error?: string;
      };

      if (result.success) {
        return {
          success: true,
          sandboxId: result.data?.sandbox_id,
          providerObjectId: result.data?.pod_name,
        };
      }

      return { success: false, error: result.error || "Unknown restore error" };
    } catch (error) {
      if (error instanceof SandboxProviderError) throw error;
      throw this.classifyError("Failed to restore sandbox from snapshot", error);
    }
  }

  /**
   * Take a VolumeSnapshot of a sandbox's workspace.
   */
  async takeSnapshot(config: SnapshotConfig): Promise<SnapshotResult> {
    try {
      const authToken = await generateInternalToken(this.secret);

      const headers: Record<string, string> = {
        "Content-Type": "application/json",
        Authorization: `Bearer ${authToken}`,
      };
      if (config.traceId) headers["x-trace-id"] = config.traceId;
      if (config.requestId) headers["x-request-id"] = config.requestId;

      const response = await fetch(`${this.sandboxApiUrl}/api/snapshot-sandbox`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          sandbox_id: config.providerObjectId,
          session_id: config.sessionId,
          reason: config.reason,
        }),
      });

      if (!response.ok) {
        throw this.classifyErrorWithStatus(
          `Snapshot failed with HTTP ${response.status}`,
          response.status
        );
      }

      const result = (await response.json()) as {
        success: boolean;
        data?: { image_id: string };
        error?: string;
      };

      if (result.success && result.data?.image_id) {
        return { success: true, imageId: result.data.image_id };
      }

      return { success: false, error: result.error || "Unknown snapshot error" };
    } catch (error) {
      if (error instanceof SandboxProviderError) throw error;
      throw this.classifyError("Failed to take snapshot", error);
    }
  }

  private classifyErrorWithStatus(message: string, status: number): SandboxProviderError {
    if (status === 502 || status === 503 || status === 504) {
      return new SandboxProviderError(message, "transient");
    }
    return new SandboxProviderError(message, "permanent");
  }

  private classifyError(message: string, error: unknown): SandboxProviderError {
    if (error instanceof Error) {
      const msg = error.message.toLowerCase();
      if (
        msg.includes("fetch failed") ||
        msg.includes("etimedout") ||
        msg.includes("econnreset") ||
        msg.includes("econnrefused") ||
        msg.includes("network") ||
        msg.includes("timeout")
      ) {
        return new SandboxProviderError(`${message}: ${error.message}`, "transient", error);
      }
    }
    return new SandboxProviderError(
      `${message}: ${error instanceof Error ? error.message : String(error)}`,
      "permanent",
      error instanceof Error ? error : undefined
    );
  }
}

/**
 * Create a K8s sandbox provider.
 *
 * @param sandboxApiUrl - URL of the sandbox-api K8s service
 * @param secret - API_SECRET for HMAC authentication
 */
export function createK8sProvider(sandboxApiUrl: string, secret: string): K8sSandboxProvider {
  return new K8sSandboxProvider(sandboxApiUrl, secret);
}
