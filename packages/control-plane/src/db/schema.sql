-- PostgreSQL schema for Open-Inspect Control Plane
-- Replaces Cloudflare D1 tables and Durable Object SQLite storage

-- Session index (replaces D1 sessions table)
CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  title TEXT,
  repo_owner TEXT NOT NULL,
  repo_name TEXT NOT NULL,
  repo_id INTEGER,
  repo_default_branch TEXT DEFAULT 'main',
  branch_name TEXT,
  base_sha TEXT,
  current_sha TEXT,
  opencode_session_id TEXT,
  model TEXT DEFAULT 'claude-haiku-4-5',
  reasoning_effort TEXT,
  status TEXT NOT NULL DEFAULT 'created',
  sandbox_status TEXT DEFAULT 'pending',
  sandbox_id TEXT,
  sandbox_auth_token TEXT,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status);
CREATE INDEX IF NOT EXISTS idx_sessions_created_at ON sessions(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sessions_repo ON sessions(repo_owner, repo_name);

-- Participants
CREATE TABLE IF NOT EXISTS participants (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  user_id TEXT NOT NULL,
  github_login TEXT,
  github_name TEXT,
  github_email TEXT,
  github_token_encrypted TEXT,
  github_refresh_token_encrypted TEXT,
  github_token_expires_at BIGINT,
  role TEXT NOT NULL DEFAULT 'member',
  joined_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_participants_session ON participants(session_id);
CREATE INDEX IF NOT EXISTS idx_participants_user ON participants(user_id);

-- Messages (prompts)
CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  author_id TEXT NOT NULL,
  content TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'web',
  model TEXT,
  reasoning_effort TEXT,
  status TEXT NOT NULL DEFAULT 'pending',
  callback_context JSONB,
  created_at BIGINT NOT NULL,
  started_at BIGINT,
  completed_at BIGINT
);

CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, created_at);

-- Events (sandbox events)
CREATE TABLE IF NOT EXISTS events (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  type TEXT NOT NULL,
  data JSONB NOT NULL DEFAULT '{}',
  message_id TEXT,
  created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_session_cursor ON events(session_id, created_at, id);

-- Artifacts (PRs, screenshots, etc.)
CREATE TABLE IF NOT EXISTS artifacts (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  type TEXT NOT NULL,
  url TEXT,
  pr_number INTEGER,
  metadata JSONB,
  created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_artifacts_session ON artifacts(session_id);

-- Repository metadata (replaces D1 repo_metadata table)
CREATE TABLE IF NOT EXISTS repo_metadata (
  repo_owner TEXT NOT NULL,
  repo_name TEXT NOT NULL,
  description TEXT,
  aliases JSONB DEFAULT '[]',
  channel_associations JSONB DEFAULT '[]',
  keywords JSONB DEFAULT '[]',
  updated_at BIGINT NOT NULL,
  PRIMARY KEY (repo_owner, repo_name)
);

-- Repository secrets (replaces D1 repo_secrets table)
CREATE TABLE IF NOT EXISTS repo_secrets (
  id SERIAL PRIMARY KEY,
  repo_id INTEGER NOT NULL,
  repo_owner TEXT NOT NULL,
  repo_name TEXT NOT NULL,
  key TEXT NOT NULL,
  encrypted_value TEXT NOT NULL,
  iv TEXT NOT NULL,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL,
  UNIQUE(repo_id, key)
);

CREATE INDEX IF NOT EXISTS idx_repo_secrets_repo ON repo_secrets(repo_id);

-- WebSocket client tokens (for authentication)
CREATE TABLE IF NOT EXISTS ws_tokens (
  token_hash TEXT PRIMARY KEY,
  session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  user_id TEXT NOT NULL,
  github_login TEXT,
  github_name TEXT,
  github_email TEXT,
  github_token_encrypted TEXT,
  github_refresh_token_encrypted TEXT,
  github_token_expires_at BIGINT,
  created_at BIGINT NOT NULL,
  expires_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ws_tokens_expires ON ws_tokens(expires_at);
