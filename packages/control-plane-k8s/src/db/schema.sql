-- Open-Inspect Control Plane — PostgreSQL Schema
--
-- Combines:
--   1. D1 shared tables (sessions index, repo_metadata, repo_secrets)
--   2. Per-session tables from DO SQLite (session_state, participants, messages, events, artifacts, sandbox, ws_client_mapping)
--
-- In the Cloudflare version each session got its own isolated SQLite database
-- inside a Durable Object.  On Kubernetes we use a single PostgreSQL database
-- with a `session_id` foreign key to scope per-session data.

-- ============================================================================
-- SHARED TABLES (previously D1)
-- ============================================================================

-- Session index for listing/filtering — replaces D1 `sessions` table.
CREATE TABLE IF NOT EXISTS sessions (
  id             TEXT PRIMARY KEY,
  title          TEXT,
  repo_owner     TEXT NOT NULL,
  repo_name      TEXT NOT NULL,
  model          TEXT NOT NULL DEFAULT 'claude-haiku-4-5',
  status         TEXT NOT NULL DEFAULT 'created',
  created_at     BIGINT NOT NULL,
  updated_at     BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status);
CREATE INDEX IF NOT EXISTS idx_sessions_updated ON sessions(updated_at DESC);

-- Repository metadata — replaces D1 `repo_metadata` table.
CREATE TABLE IF NOT EXISTS repo_metadata (
  repo_owner              TEXT NOT NULL,
  repo_name               TEXT NOT NULL,
  description             TEXT,
  aliases                 JSONB,           -- JSON array of strings
  channel_associations    JSONB,           -- JSON array of strings
  keywords                JSONB,           -- JSON array of strings
  created_at              BIGINT NOT NULL,
  updated_at              BIGINT NOT NULL,
  PRIMARY KEY (repo_owner, repo_name)
);

-- Encrypted repository secrets — replaces D1 `repo_secrets` table.
CREATE TABLE IF NOT EXISTS repo_secrets (
  repo_id          INTEGER NOT NULL,
  repo_owner       TEXT NOT NULL,
  repo_name        TEXT NOT NULL,
  key              TEXT NOT NULL,
  encrypted_value  TEXT NOT NULL,
  created_at       BIGINT NOT NULL,
  updated_at       BIGINT NOT NULL,
  PRIMARY KEY (repo_id, key)
);

CREATE INDEX IF NOT EXISTS idx_repo_secrets_repo ON repo_secrets(repo_id);

-- ============================================================================
-- PER-SESSION TABLES (previously per-DO SQLite)
-- ============================================================================
-- All per-session tables use `session_id` as a foreign key to the `sessions`
-- table's `id` column.

-- Core session state — replaces DO SQLite `session` table.
CREATE TABLE IF NOT EXISTS session_state (
  session_id            TEXT PRIMARY KEY REFERENCES sessions(id) ON DELETE CASCADE,
  session_name          TEXT,                         -- External name for WS routing
  title                 TEXT,
  repo_owner            TEXT NOT NULL,
  repo_name             TEXT NOT NULL,
  repo_id               INTEGER,
  repo_default_branch   TEXT NOT NULL DEFAULT 'main',
  branch_name           TEXT,
  base_sha              TEXT,
  current_sha           TEXT,
  opencode_session_id   TEXT,
  model                 TEXT DEFAULT 'claude-haiku-4-5',
  reasoning_effort      TEXT,
  status                TEXT DEFAULT 'created',
  created_at            BIGINT NOT NULL,
  updated_at            BIGINT NOT NULL
);

-- Session participants.
CREATE TABLE IF NOT EXISTS participants (
  id                              TEXT PRIMARY KEY,
  session_id                      TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  user_id                         TEXT NOT NULL,
  github_user_id                  TEXT,
  github_login                    TEXT,
  github_email                    TEXT,
  github_name                     TEXT,
  role                            TEXT NOT NULL DEFAULT 'member',
  github_access_token_encrypted   TEXT,
  github_refresh_token_encrypted  TEXT,
  github_token_expires_at         BIGINT,
  ws_auth_token                   TEXT,
  ws_token_created_at             BIGINT,
  joined_at                       BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_participants_session ON participants(session_id);
CREATE INDEX IF NOT EXISTS idx_participants_user ON participants(session_id, user_id);

-- Message queue and history.
CREATE TABLE IF NOT EXISTS messages (
  id                TEXT PRIMARY KEY,
  session_id        TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  author_id         TEXT NOT NULL,
  content           TEXT NOT NULL,
  source            TEXT NOT NULL,
  model             TEXT,
  reasoning_effort  TEXT,
  attachments       JSONB,
  callback_context  JSONB,
  status            TEXT DEFAULT 'pending',
  error_message     TEXT,
  created_at        BIGINT NOT NULL,
  started_at        BIGINT,
  completed_at      BIGINT
);

CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id);
CREATE INDEX IF NOT EXISTS idx_messages_status ON messages(session_id, status);

-- Agent event log.
CREATE TABLE IF NOT EXISTS events (
  id          TEXT PRIMARY KEY,
  session_id  TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  type        TEXT NOT NULL,
  data        JSONB NOT NULL,
  message_id  TEXT,
  created_at  BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id);
CREATE INDEX IF NOT EXISTS idx_events_created ON events(session_id, created_at, id);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(session_id, type);

-- Artifacts (PRs, screenshots, preview URLs).
CREATE TABLE IF NOT EXISTS artifacts (
  id          TEXT PRIMARY KEY,
  session_id  TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  type        TEXT NOT NULL,
  url         TEXT,
  metadata    JSONB,
  created_at  BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_artifacts_session ON artifacts(session_id);

-- Sandbox state — one row per session.
CREATE TABLE IF NOT EXISTS sandbox (
  id                    TEXT PRIMARY KEY,
  session_id            TEXT NOT NULL UNIQUE REFERENCES sessions(id) ON DELETE CASCADE,
  modal_sandbox_id      TEXT,
  modal_object_id       TEXT,
  snapshot_id           TEXT,
  snapshot_image_id     TEXT,
  auth_token            TEXT,
  status                TEXT DEFAULT 'pending',
  git_sync_status       TEXT DEFAULT 'pending',
  last_heartbeat        BIGINT,
  last_activity         BIGINT,
  last_spawn_error      TEXT,
  last_spawn_error_at   BIGINT,
  spawn_failure_count   INTEGER DEFAULT 0,
  last_spawn_failure    BIGINT,
  created_at            BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sandbox_session ON sandbox(session_id);

-- WebSocket client mapping for reconnection recovery.
CREATE TABLE IF NOT EXISTS ws_client_mapping (
  ws_id           TEXT PRIMARY KEY,
  session_id      TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  participant_id  TEXT NOT NULL,
  client_id       TEXT,
  created_at      BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ws_mapping_session ON ws_client_mapping(session_id);
