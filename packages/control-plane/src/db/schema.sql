-- Open-Inspect PostgreSQL Schema
--
-- Replaces:
-- 1. Cloudflare D1 tables (sessions index, repo_metadata, repo_secrets)
-- 2. Durable Object per-session SQLite (session, participants, messages, events, artifacts, sandbox)
--
-- All data is now in a single PostgreSQL database with session_id foreign keys.

-- Core session state (was: per-DO session table + D1 sessions index)
CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  session_name TEXT,
  title TEXT,
  repo_owner TEXT NOT NULL,
  repo_name TEXT NOT NULL,
  repo_id INTEGER,
  repo_default_branch TEXT NOT NULL DEFAULT 'main',
  branch_name TEXT,
  base_sha TEXT,
  current_sha TEXT,
  opencode_session_id TEXT,
  model TEXT DEFAULT 'claude-haiku-4-5',
  reasoning_effort TEXT,
  status TEXT DEFAULT 'created' CHECK (status IN ('created', 'active', 'completed', 'archived')),
  -- Fields from the D1 session index
  owner_user_id TEXT,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status);
CREATE INDEX IF NOT EXISTS idx_sessions_owner ON sessions(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_repo ON sessions(repo_owner, repo_name);
CREATE INDEX IF NOT EXISTS idx_sessions_updated ON sessions(updated_at DESC);

-- Participants in sessions (was: per-DO participants table)
CREATE TABLE IF NOT EXISTS participants (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  user_id TEXT NOT NULL,
  github_user_id TEXT,
  github_login TEXT,
  github_email TEXT,
  github_name TEXT,
  role TEXT NOT NULL DEFAULT 'member' CHECK (role IN ('owner', 'member')),
  github_access_token_encrypted TEXT,
  github_refresh_token_encrypted TEXT,
  github_token_expires_at BIGINT,
  ws_auth_token TEXT,
  ws_token_created_at BIGINT,
  joined_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_participants_session ON participants(session_id);
CREATE INDEX IF NOT EXISTS idx_participants_user ON participants(user_id);

-- Message queue and history (was: per-DO messages table)
CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  author_id TEXT NOT NULL,
  content TEXT NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('web', 'slack', 'extension', 'github')),
  model TEXT,
  reasoning_effort TEXT,
  attachments TEXT, -- JSON array
  callback_context TEXT, -- JSON
  status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
  error_message TEXT,
  created_at BIGINT NOT NULL,
  started_at BIGINT,
  completed_at BIGINT
);

CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id);
CREATE INDEX IF NOT EXISTS idx_messages_status ON messages(session_id, status);
CREATE INDEX IF NOT EXISTS idx_messages_author ON messages(author_id);

-- Agent event log (was: per-DO events table)
CREATE TABLE IF NOT EXISTS events (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  type TEXT NOT NULL,
  data TEXT NOT NULL, -- JSON payload
  message_id TEXT,
  created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id);
CREATE INDEX IF NOT EXISTS idx_events_message ON events(session_id, message_id);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(session_id, type);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(session_id, created_at, id);

-- Artifacts (PRs, screenshots, preview URLs) (was: per-DO artifacts table)
CREATE TABLE IF NOT EXISTS artifacts (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  type TEXT NOT NULL CHECK (type IN ('pr', 'screenshot', 'preview', 'branch')),
  url TEXT,
  metadata TEXT, -- JSON
  created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_artifacts_session ON artifacts(session_id);

-- Sandbox state (was: per-DO sandbox table)
-- Renamed modal-specific columns to be provider-agnostic
CREATE TABLE IF NOT EXISTS sandboxes (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  sandbox_id TEXT,          -- Our generated sandbox ID (was: modal_sandbox_id)
  provider_object_id TEXT,  -- Provider's internal ID: K8s pod name (was: modal_object_id)
  snapshot_id TEXT,
  snapshot_image_id TEXT,   -- VolumeSnapshot name (was: Modal Image ID)
  auth_token TEXT,
  status TEXT DEFAULT 'pending',
  git_sync_status TEXT DEFAULT 'pending',
  last_heartbeat BIGINT,
  last_activity BIGINT,
  last_spawn_error TEXT,
  last_spawn_error_at BIGINT,
  spawn_failure_count INTEGER DEFAULT 0,
  last_spawn_failure BIGINT,
  created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sandboxes_session ON sandboxes(session_id);

-- Repository metadata (was: D1 repo_metadata table)
CREATE TABLE IF NOT EXISTS repo_metadata (
  repo_owner TEXT NOT NULL,
  repo_name TEXT NOT NULL,
  description TEXT,
  aliases TEXT,              -- JSON array
  channel_association TEXT,  -- JSON
  keywords TEXT,             -- JSON array
  updated_at BIGINT,
  PRIMARY KEY (repo_owner, repo_name)
);

-- Repository-scoped encrypted secrets (was: D1 repo_secrets table)
CREATE TABLE IF NOT EXISTS repo_secrets (
  repo_owner TEXT NOT NULL,
  repo_name TEXT NOT NULL,
  key TEXT NOT NULL,
  value_encrypted TEXT NOT NULL,
  repo_id TEXT,
  updated_at BIGINT NOT NULL,
  PRIMARY KEY (repo_owner, repo_name, key)
);

CREATE INDEX IF NOT EXISTS idx_repo_secrets_repo ON repo_secrets(repo_owner, repo_name);

-- WebSocket client mapping for session recovery
-- In the Rivet actor model, actors may hibernate and need to recover client identity
CREATE TABLE IF NOT EXISTS ws_client_mapping (
  ws_id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  participant_id TEXT NOT NULL,
  client_id TEXT,
  created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ws_mapping_session ON ws_client_mapping(session_id);
