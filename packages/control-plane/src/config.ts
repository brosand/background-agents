/**
 * Environment configuration for Open-Inspect Control Plane (K8s/Node.js).
 *
 * Replaces Cloudflare Workers Env bindings with process.env configuration.
 */

export interface Config {
  port: number;
  host: string;

  // Database
  databaseUrl: string;

  // Cache
  redisUrl: string;

  // Rivet sandbox orchestration
  rivetApiUrl: string;
  rivetToken: string;
  rivetProject: string;
  rivetEnvironment: string;
  rivetSandboxBuildTag: string;

  // GitHub OAuth
  githubClientId: string;
  githubClientSecret: string;

  // GitHub App
  githubAppId: string;
  githubAppPrivateKey: string;
  githubAppInstallationId: string;

  // Encryption
  tokenEncryptionKey: string;
  repoSecretsEncryptionKey: string;

  // Internal auth
  internalCallbackSecret: string;

  // Deployment
  deploymentName: string;
  workerUrl: string;
  webAppUrl: string;
  scmProvider: string;

  // Sandbox lifecycle
  sandboxInactivityTimeoutMs: number;

  // Logging
  logLevel: string;
}

function requireEnv(key: string): string {
  const value = process.env[key];
  if (!value) {
    throw new Error(`Required environment variable ${key} is not set`);
  }
  return value;
}

function optionalEnv(key: string, defaultValue = ""): string {
  return process.env[key] || defaultValue;
}

export function loadConfig(): Config {
  return {
    port: parseInt(optionalEnv("PORT", "3001"), 10),
    host: optionalEnv("HOST", "0.0.0.0"),

    databaseUrl: requireEnv("DATABASE_URL"),
    redisUrl: optionalEnv("REDIS_URL", "redis://localhost:6379"),

    rivetApiUrl: requireEnv("RIVET_API_URL"),
    rivetToken: requireEnv("RIVET_TOKEN"),
    rivetProject: requireEnv("RIVET_PROJECT"),
    rivetEnvironment: optionalEnv("RIVET_ENVIRONMENT", "production"),
    rivetSandboxBuildTag: optionalEnv("RIVET_SANDBOX_BUILD_TAG", "latest"),

    githubClientId: optionalEnv("GITHUB_CLIENT_ID"),
    githubClientSecret: optionalEnv("GITHUB_CLIENT_SECRET"),

    githubAppId: optionalEnv("GITHUB_APP_ID"),
    githubAppPrivateKey: optionalEnv("GITHUB_APP_PRIVATE_KEY"),
    githubAppInstallationId: optionalEnv("GITHUB_APP_INSTALLATION_ID"),

    tokenEncryptionKey: requireEnv("TOKEN_ENCRYPTION_KEY"),
    repoSecretsEncryptionKey: optionalEnv("REPO_SECRETS_ENCRYPTION_KEY"),

    internalCallbackSecret: requireEnv("INTERNAL_CALLBACK_SECRET"),

    deploymentName: optionalEnv("DEPLOYMENT_NAME", "open-inspect"),
    workerUrl: optionalEnv("WORKER_URL", "http://localhost:3001"),
    webAppUrl: optionalEnv("WEB_APP_URL", "http://localhost:3000"),
    scmProvider: optionalEnv("SCM_PROVIDER", "github"),

    sandboxInactivityTimeoutMs: parseInt(
      optionalEnv("SANDBOX_INACTIVITY_TIMEOUT_MS", "600000"),
      10
    ),

    logLevel: optionalEnv("LOG_LEVEL", "info"),
  };
}
