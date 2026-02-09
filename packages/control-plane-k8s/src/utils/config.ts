/**
 * Configuration — replaces Cloudflare Worker Env bindings.
 *
 * All configuration comes from environment variables (standard K8s pattern).
 * In the Cloudflare version these were typed bindings on the Env interface;
 * here they're loaded from process.env.
 */

export interface Config {
  // Database
  databaseUrl: string;
  redisUrl: string;

  // Encryption
  tokenEncryptionKey: string;
  repoSecretsEncryptionKey?: string;

  // Modal
  modalApiSecret?: string;
  modalWorkspace?: string;
  modalApiUrl?: string;

  // GitHub App
  githubAppId?: string;
  githubAppPrivateKey?: string;
  githubAppInstallationId?: string;

  // GitHub OAuth
  githubClientId?: string;
  githubClientSecret?: string;

  // URLs
  workerUrl?: string;
  webAppUrl?: string;

  // Internal
  internalCallbackSecret?: string;
  deploymentName: string;

  // Server
  port: number;
  host: string;

  // Sandbox
  sandboxInactivityTimeoutMs: number;
}

export function loadConfig(): Config {
  const env = process.env;

  return {
    databaseUrl: env.DATABASE_URL || "postgresql://localhost:5432/open_inspect",
    redisUrl: env.REDIS_URL || "redis://localhost:6379",

    tokenEncryptionKey: env.TOKEN_ENCRYPTION_KEY || "",
    repoSecretsEncryptionKey: env.REPO_SECRETS_ENCRYPTION_KEY,

    modalApiSecret: env.MODAL_API_SECRET,
    modalWorkspace: env.MODAL_WORKSPACE,
    modalApiUrl: env.MODAL_API_URL,

    githubAppId: env.GITHUB_APP_ID,
    githubAppPrivateKey: env.GITHUB_APP_PRIVATE_KEY,
    githubAppInstallationId: env.GITHUB_APP_INSTALLATION_ID,

    githubClientId: env.GITHUB_CLIENT_ID,
    githubClientSecret: env.GITHUB_CLIENT_SECRET,

    workerUrl: env.WORKER_URL,
    webAppUrl: env.WEB_APP_URL,

    internalCallbackSecret: env.INTERNAL_CALLBACK_SECRET,
    deploymentName: env.DEPLOYMENT_NAME || "default",

    port: parseInt(env.PORT || "8080", 10),
    host: env.HOST || "0.0.0.0",

    sandboxInactivityTimeoutMs: parseInt(env.SANDBOX_INACTIVITY_TIMEOUT_MS || "600000", 10),
  };
}
