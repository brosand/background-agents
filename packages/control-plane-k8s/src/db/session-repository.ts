/**
 * SessionRepository — PostgreSQL replacement for the per-DO SQLite repository.
 *
 * In the Cloudflare version, each Durable Object had its own SQLite database
 * with single-row session/sandbox tables. Here we use PostgreSQL with
 * session_id scoping.
 *
 * All methods are async (unlike the synchronous SQLite originals) because
 * PostgreSQL queries are async.
 */

import type pg from "pg";

// Re-export types from the original codebase that stay the same
export interface SessionRow {
  session_id: string;
  session_name: string | null;
  title: string | null;
  repo_owner: string;
  repo_name: string;
  repo_id: number | null;
  repo_default_branch: string;
  branch_name: string | null;
  base_sha: string | null;
  current_sha: string | null;
  opencode_session_id: string | null;
  model: string;
  reasoning_effort: string | null;
  status: string;
  created_at: string;
  updated_at: string;
}

export interface ParticipantRow {
  id: string;
  session_id: string;
  user_id: string;
  github_user_id: string | null;
  github_login: string | null;
  github_email: string | null;
  github_name: string | null;
  role: string;
  github_access_token_encrypted: string | null;
  github_refresh_token_encrypted: string | null;
  github_token_expires_at: string | null;
  ws_auth_token: string | null;
  ws_token_created_at: string | null;
  joined_at: string;
}

export interface MessageRow {
  id: string;
  session_id: string;
  author_id: string;
  content: string;
  source: string;
  model: string | null;
  reasoning_effort: string | null;
  attachments: unknown;
  callback_context: unknown;
  status: string;
  error_message: string | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
}

export interface EventRow {
  id: string;
  session_id: string;
  type: string;
  data: unknown;
  message_id: string | null;
  created_at: string;
}

export interface ArtifactRow {
  id: string;
  session_id: string;
  type: string;
  url: string | null;
  metadata: unknown;
  created_at: string;
}

export interface SandboxRow {
  id: string;
  session_id: string;
  modal_sandbox_id: string | null;
  modal_object_id: string | null;
  snapshot_id: string | null;
  snapshot_image_id: string | null;
  auth_token: string | null;
  status: string;
  git_sync_status: string;
  last_heartbeat: string | null;
  last_activity: string | null;
  last_spawn_error: string | null;
  last_spawn_error_at: string | null;
  spawn_failure_count: number;
  last_spawn_failure: string | null;
  created_at: string;
}

export interface WsClientMappingResult {
  participant_id: string;
  client_id: string;
  user_id: string;
  github_name: string | null;
  github_login: string | null;
}

export class SessionRepository {
  constructor(
    private readonly pool: pg.Pool,
    private readonly sessionId: string
  ) {}

  // === SESSION STATE ===

  async getSession(): Promise<SessionRow | null> {
    const result = await this.pool.query<SessionRow>(
      "SELECT * FROM session_state WHERE session_id = $1",
      [this.sessionId]
    );
    return result.rows[0] ?? null;
  }

  async upsertSession(data: {
    sessionName: string;
    title: string | null;
    repoOwner: string;
    repoName: string;
    repoId?: number | null;
    model: string;
    reasoningEffort?: string | null;
    status: string;
    createdAt: number;
    updatedAt: number;
  }): Promise<void> {
    await this.pool.query(
      `INSERT INTO session_state (session_id, session_name, title, repo_owner, repo_name, repo_id, model, reasoning_effort, status, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
       ON CONFLICT (session_id) DO UPDATE SET
         session_name = EXCLUDED.session_name,
         title = EXCLUDED.title,
         repo_owner = EXCLUDED.repo_owner,
         repo_name = EXCLUDED.repo_name,
         repo_id = EXCLUDED.repo_id,
         model = EXCLUDED.model,
         reasoning_effort = EXCLUDED.reasoning_effort,
         status = EXCLUDED.status,
         updated_at = EXCLUDED.updated_at`,
      [
        this.sessionId,
        data.sessionName,
        data.title,
        data.repoOwner,
        data.repoName,
        data.repoId ?? null,
        data.model,
        data.reasoningEffort ?? null,
        data.status,
        data.createdAt,
        data.updatedAt,
      ]
    );
  }

  async updateSessionBranch(branchName: string): Promise<void> {
    await this.pool.query(
      "UPDATE session_state SET branch_name = $1 WHERE session_id = $2",
      [branchName, this.sessionId]
    );
  }

  async updateSessionCurrentSha(sha: string): Promise<void> {
    await this.pool.query(
      "UPDATE session_state SET current_sha = $1 WHERE session_id = $2",
      [sha, this.sessionId]
    );
  }

  async updateSessionStatus(status: string, updatedAt: number): Promise<void> {
    await this.pool.query(
      "UPDATE session_state SET status = $1, updated_at = $2 WHERE session_id = $3",
      [status, updatedAt, this.sessionId]
    );
  }

  async updateSessionRepoId(repoId: number): Promise<void> {
    await this.pool.query(
      "UPDATE session_state SET repo_id = $1 WHERE session_id = $2",
      [repoId, this.sessionId]
    );
  }

  // === SANDBOX ===

  async getSandbox(): Promise<SandboxRow | null> {
    const result = await this.pool.query<SandboxRow>(
      "SELECT * FROM sandbox WHERE session_id = $1",
      [this.sessionId]
    );
    return result.rows[0] ?? null;
  }

  async createSandbox(data: {
    id: string;
    status: string;
    gitSyncStatus: string;
    createdAt: number;
  }): Promise<void> {
    await this.pool.query(
      `INSERT INTO sandbox (id, session_id, status, git_sync_status, created_at)
       VALUES ($1, $2, $3, $4, $5)`,
      [data.id, this.sessionId, data.status, data.gitSyncStatus, data.createdAt]
    );
  }

  async updateSandboxStatus(status: string): Promise<void> {
    await this.pool.query(
      "UPDATE sandbox SET status = $1 WHERE session_id = $2",
      [status, this.sessionId]
    );
  }

  async updateSandboxForSpawn(data: {
    status: string;
    createdAt: number;
    authToken: string;
    modalSandboxId: string;
  }): Promise<void> {
    await this.pool.query(
      `UPDATE sandbox SET status = $1, created_at = $2, auth_token = $3, modal_sandbox_id = $4
       WHERE session_id = $5`,
      [data.status, data.createdAt, data.authToken, data.modalSandboxId, this.sessionId]
    );
  }

  async updateSandboxHeartbeat(timestamp: number): Promise<void> {
    await this.pool.query(
      "UPDATE sandbox SET last_heartbeat = $1 WHERE session_id = $2",
      [timestamp, this.sessionId]
    );
  }

  async updateSandboxLastActivity(timestamp: number): Promise<void> {
    await this.pool.query(
      "UPDATE sandbox SET last_activity = $1 WHERE session_id = $2",
      [timestamp, this.sessionId]
    );
  }

  async updateSandboxGitSyncStatus(status: string): Promise<void> {
    await this.pool.query(
      "UPDATE sandbox SET git_sync_status = $1 WHERE session_id = $2",
      [status, this.sessionId]
    );
  }

  async updateSandboxSnapshotImageId(sandboxId: string, imageId: string): Promise<void> {
    await this.pool.query(
      "UPDATE sandbox SET snapshot_image_id = $1 WHERE id = $2",
      [imageId, sandboxId]
    );
  }

  async updateSandboxModalObjectId(modalObjectId: string): Promise<void> {
    await this.pool.query(
      "UPDATE sandbox SET modal_object_id = $1 WHERE session_id = $2",
      [modalObjectId, this.sessionId]
    );
  }

  async updateSandboxSpawnError(error: string | null, timestamp: number | null): Promise<void> {
    await this.pool.query(
      "UPDATE sandbox SET last_spawn_error = $1, last_spawn_error_at = $2 WHERE session_id = $3",
      [error, timestamp, this.sessionId]
    );
  }

  async resetCircuitBreaker(): Promise<void> {
    await this.pool.query(
      "UPDATE sandbox SET spawn_failure_count = 0 WHERE session_id = $1",
      [this.sessionId]
    );
  }

  async incrementCircuitBreakerFailure(timestamp: number): Promise<void> {
    await this.pool.query(
      `UPDATE sandbox SET
         spawn_failure_count = COALESCE(spawn_failure_count, 0) + 1,
         last_spawn_failure = $1
       WHERE session_id = $2`,
      [timestamp, this.sessionId]
    );
  }

  // === PARTICIPANTS ===

  async getParticipantByUserId(userId: string): Promise<ParticipantRow | null> {
    const result = await this.pool.query<ParticipantRow>(
      "SELECT * FROM participants WHERE session_id = $1 AND user_id = $2",
      [this.sessionId, userId]
    );
    return result.rows[0] ?? null;
  }

  async getParticipantByWsTokenHash(tokenHash: string): Promise<ParticipantRow | null> {
    const result = await this.pool.query<ParticipantRow>(
      "SELECT * FROM participants WHERE session_id = $1 AND ws_auth_token = $2",
      [this.sessionId, tokenHash]
    );
    return result.rows[0] ?? null;
  }

  async getParticipantById(participantId: string): Promise<ParticipantRow | null> {
    const result = await this.pool.query<ParticipantRow>(
      "SELECT * FROM participants WHERE id = $1",
      [participantId]
    );
    return result.rows[0] ?? null;
  }

  async createParticipant(data: {
    id: string;
    userId: string;
    githubUserId?: string | null;
    githubLogin?: string | null;
    githubName?: string | null;
    githubEmail?: string | null;
    githubAccessTokenEncrypted?: string | null;
    githubRefreshTokenEncrypted?: string | null;
    githubTokenExpiresAt?: number | null;
    role: string;
    joinedAt: number;
  }): Promise<void> {
    await this.pool.query(
      `INSERT INTO participants (id, session_id, user_id, github_user_id, github_login, github_name, github_email,
         github_access_token_encrypted, github_refresh_token_encrypted, github_token_expires_at, role, joined_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
      [
        data.id,
        this.sessionId,
        data.userId,
        data.githubUserId ?? null,
        data.githubLogin ?? null,
        data.githubName ?? null,
        data.githubEmail ?? null,
        data.githubAccessTokenEncrypted ?? null,
        data.githubRefreshTokenEncrypted ?? null,
        data.githubTokenExpiresAt ?? null,
        data.role,
        data.joinedAt,
      ]
    );
  }

  async updateParticipantWsToken(participantId: string, tokenHash: string, createdAt: number): Promise<void> {
    await this.pool.query(
      "UPDATE participants SET ws_auth_token = $1, ws_token_created_at = $2 WHERE id = $3",
      [tokenHash, createdAt, participantId]
    );
  }

  async listParticipants(): Promise<ParticipantRow[]> {
    const result = await this.pool.query<ParticipantRow>(
      "SELECT * FROM participants WHERE session_id = $1 ORDER BY joined_at",
      [this.sessionId]
    );
    return result.rows;
  }

  // === MESSAGES ===

  async getMessageCount(): Promise<number> {
    const result = await this.pool.query<{ count: string }>(
      "SELECT COUNT(*) as count FROM messages WHERE session_id = $1",
      [this.sessionId]
    );
    return Number(result.rows[0].count);
  }

  async getPendingOrProcessingCount(): Promise<number> {
    const result = await this.pool.query<{ count: string }>(
      "SELECT COUNT(*) as count FROM messages WHERE session_id = $1 AND status IN ('pending', 'processing')",
      [this.sessionId]
    );
    return Number(result.rows[0].count);
  }

  async getProcessingMessage(): Promise<{ id: string } | null> {
    const result = await this.pool.query<{ id: string }>(
      "SELECT id FROM messages WHERE session_id = $1 AND status = 'processing' LIMIT 1",
      [this.sessionId]
    );
    return result.rows[0] ?? null;
  }

  async getNextPendingMessage(): Promise<MessageRow | null> {
    const result = await this.pool.query<MessageRow>(
      "SELECT * FROM messages WHERE session_id = $1 AND status = 'pending' ORDER BY created_at ASC LIMIT 1",
      [this.sessionId]
    );
    return result.rows[0] ?? null;
  }

  async createMessage(data: {
    id: string;
    authorId: string;
    content: string;
    source: string;
    model?: string | null;
    reasoningEffort?: string | null;
    attachments?: string | null;
    callbackContext?: string | null;
    status: string;
    createdAt: number;
  }): Promise<void> {
    await this.pool.query(
      `INSERT INTO messages (id, session_id, author_id, content, source, model, reasoning_effort, attachments, callback_context, status, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
      [
        data.id,
        this.sessionId,
        data.authorId,
        data.content,
        data.source,
        data.model ?? null,
        data.reasoningEffort ?? null,
        data.attachments ?? null,
        data.callbackContext ?? null,
        data.status,
        data.createdAt,
      ]
    );
  }

  async updateMessageToProcessing(messageId: string, startedAt: number): Promise<void> {
    await this.pool.query(
      "UPDATE messages SET status = 'processing', started_at = $1 WHERE id = $2",
      [startedAt, messageId]
    );
  }

  async updateMessageCompletion(messageId: string, status: string, completedAt: number): Promise<void> {
    await this.pool.query(
      "UPDATE messages SET status = $1, completed_at = $2 WHERE id = $3",
      [status, completedAt, messageId]
    );
  }

  async getMessageCallbackContext(messageId: string): Promise<{ callback_context: unknown } | null> {
    const result = await this.pool.query<{ callback_context: unknown }>(
      "SELECT callback_context FROM messages WHERE id = $1",
      [messageId]
    );
    return result.rows[0] ?? null;
  }

  async getMessageTimestamps(messageId: string): Promise<{ created_at: number; started_at: number | null } | null> {
    const result = await this.pool.query<{ created_at: string; started_at: string | null }>(
      "SELECT created_at, started_at FROM messages WHERE id = $1",
      [messageId]
    );
    if (!result.rows[0]) return null;
    return {
      created_at: Number(result.rows[0].created_at),
      started_at: result.rows[0].started_at ? Number(result.rows[0].started_at) : null,
    };
  }

  // === EVENTS ===

  async createEvent(data: {
    id: string;
    type: string;
    data: string;
    messageId: string | null;
    createdAt: number;
  }): Promise<void> {
    await this.pool.query(
      `INSERT INTO events (id, session_id, type, data, message_id, created_at)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [data.id, this.sessionId, data.type, data.data, data.messageId, data.createdAt]
    );
  }

  async getEventsForReplay(limit: number): Promise<EventRow[]> {
    const result = await this.pool.query<EventRow>(
      `SELECT * FROM (
         SELECT * FROM events WHERE session_id = $1 AND type != 'heartbeat'
         ORDER BY created_at DESC, id DESC LIMIT $2
       ) sub ORDER BY created_at ASC, id ASC`,
      [this.sessionId, limit]
    );
    return result.rows;
  }

  async getEventsHistoryPage(
    cursorTimestamp: number,
    cursorId: string,
    limit: number
  ): Promise<{ events: EventRow[]; hasMore: boolean }> {
    const result = await this.pool.query<EventRow>(
      `SELECT * FROM events
       WHERE session_id = $1 AND type != 'heartbeat'
         AND ((created_at < $2) OR (created_at = $2 AND id < $3))
       ORDER BY created_at DESC, id DESC LIMIT $4`,
      [this.sessionId, cursorTimestamp, cursorId, limit + 1]
    );

    const rows = result.rows;
    const hasMore = rows.length > limit;
    if (hasMore) rows.pop();
    rows.reverse();

    return { events: rows, hasMore };
  }

  // === ARTIFACTS ===

  async createArtifact(data: {
    id: string;
    type: string;
    url: string | null;
    metadata: string | null;
    createdAt: number;
  }): Promise<void> {
    await this.pool.query(
      `INSERT INTO artifacts (id, session_id, type, url, metadata, created_at)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [data.id, this.sessionId, data.type, data.url, data.metadata, data.createdAt]
    );
  }

  async listArtifacts(): Promise<ArtifactRow[]> {
    const result = await this.pool.query<ArtifactRow>(
      "SELECT * FROM artifacts WHERE session_id = $1 ORDER BY created_at DESC",
      [this.sessionId]
    );
    return result.rows;
  }

  // === WS CLIENT MAPPING ===

  async upsertWsClientMapping(data: {
    wsId: string;
    participantId: string;
    clientId: string;
    createdAt: number;
  }): Promise<void> {
    await this.pool.query(
      `INSERT INTO ws_client_mapping (ws_id, session_id, participant_id, client_id, created_at)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (ws_id) DO UPDATE SET
         participant_id = EXCLUDED.participant_id,
         client_id = EXCLUDED.client_id,
         created_at = EXCLUDED.created_at`,
      [data.wsId, this.sessionId, data.participantId, data.clientId, data.createdAt]
    );
  }

  async getWsClientMapping(wsId: string): Promise<WsClientMappingResult | null> {
    const result = await this.pool.query<WsClientMappingResult>(
      `SELECT m.participant_id, m.client_id, p.user_id, p.github_name, p.github_login
       FROM ws_client_mapping m
       JOIN participants p ON m.participant_id = p.id
       WHERE m.ws_id = $1`,
      [wsId]
    );
    return result.rows[0] ?? null;
  }

  async hasWsClientMapping(wsId: string): Promise<boolean> {
    const result = await this.pool.query<{ participant_id: string }>(
      "SELECT participant_id FROM ws_client_mapping WHERE ws_id = $1",
      [wsId]
    );
    return result.rows.length > 0;
  }

  async getProcessingMessageAuthor(): Promise<{ author_id: string } | null> {
    const result = await this.pool.query<{ author_id: string }>(
      "SELECT author_id FROM messages WHERE session_id = $1 AND status = 'processing' LIMIT 1",
      [this.sessionId]
    );
    return result.rows[0] ?? null;
  }
}
