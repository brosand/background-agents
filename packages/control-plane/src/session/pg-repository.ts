/**
 * PostgreSQL session repository.
 *
 * Replaces SessionRepository (Durable Object SqlStorage-backed).
 * All queries are scoped by session_id since data is now in a shared database.
 */

import { query, queryOne, execute } from "../db/postgres";

// --- Data interfaces ---

export interface UpsertSessionData {
  id: string;
  sessionName?: string;
  title?: string;
  repoOwner: string;
  repoName: string;
  repoId?: number;
  repoDefaultBranch?: string;
  model?: string;
  reasoningEffort?: string;
  ownerUserId?: string;
}

export interface CreateSandboxData {
  id: string;
  sessionId: string;
  sandboxId?: string;
  authToken?: string;
  status?: string;
}

export interface SpawnSandboxData {
  sandboxId: string;
  providerObjectId?: string;
  authToken: string;
  status: string;
}

export interface CreateParticipantData {
  id: string;
  sessionId: string;
  userId: string;
  githubUserId?: string;
  githubLogin?: string;
  githubEmail?: string;
  githubName?: string;
  role: string;
  githubAccessTokenEncrypted?: string;
  githubRefreshTokenEncrypted?: string;
  githubTokenExpiresAt?: number;
  wsAuthToken?: string;
  wsTokenCreatedAt?: number;
}

export interface CreateMessageData {
  id: string;
  sessionId: string;
  authorId: string;
  content: string;
  source: string;
  model?: string;
  reasoningEffort?: string;
  attachments?: string;
  callbackContext?: string;
}

export interface CreateEventData {
  id: string;
  sessionId: string;
  type: string;
  data: string;
  messageId?: string;
}

export interface CreateArtifactData {
  id: string;
  sessionId: string;
  type: string;
  url?: string;
  metadata?: string;
}

export interface ListEventsOptions {
  sessionId: string;
  limit?: number;
  afterTimestamp?: number;
  afterId?: string;
  beforeTimestamp?: number;
  beforeId?: string;
}

// --- Repository class ---

export class PgSessionRepository {
  // ===== Session =====

  async getSession(sessionId: string) {
    return queryOne(
      `SELECT * FROM sessions WHERE id = $1`,
      [sessionId]
    );
  }

  async upsertSession(data: UpsertSessionData) {
    const now = Date.now();
    await execute(
      `INSERT INTO sessions (id, session_name, title, repo_owner, repo_name, repo_id, repo_default_branch, model, reasoning_effort, owner_user_id, status, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'created', $11, $11)
       ON CONFLICT (id) DO UPDATE SET
         session_name = COALESCE(EXCLUDED.session_name, sessions.session_name),
         title = COALESCE(EXCLUDED.title, sessions.title),
         repo_id = COALESCE(EXCLUDED.repo_id, sessions.repo_id),
         model = COALESCE(EXCLUDED.model, sessions.model),
         reasoning_effort = COALESCE(EXCLUDED.reasoning_effort, sessions.reasoning_effort),
         updated_at = $11`,
      [
        data.id,
        data.sessionName ?? null,
        data.title ?? null,
        data.repoOwner,
        data.repoName,
        data.repoId ?? null,
        data.repoDefaultBranch ?? "main",
        data.model ?? "claude-haiku-4-5",
        data.reasoningEffort ?? null,
        data.ownerUserId ?? null,
        now,
      ]
    );
  }

  async updateSessionBranch(sessionId: string, branchName: string) {
    await execute(
      `UPDATE sessions SET branch_name = $1, updated_at = $2 WHERE id = $3`,
      [branchName, Date.now(), sessionId]
    );
  }

  async updateSessionCurrentSha(sessionId: string, sha: string) {
    await execute(
      `UPDATE sessions SET current_sha = $1, updated_at = $2 WHERE id = $3`,
      [sha, Date.now(), sessionId]
    );
  }

  async updateSessionStatus(sessionId: string, status: string) {
    await execute(
      `UPDATE sessions SET status = $1, updated_at = $2 WHERE id = $3`,
      [status, Date.now(), sessionId]
    );
  }

  async updateSessionRepoId(sessionId: string, repoId: number) {
    await execute(
      `UPDATE sessions SET repo_id = $1, updated_at = $2 WHERE id = $3`,
      [repoId, Date.now(), sessionId]
    );
  }

  // ===== Sandbox =====

  async getSandbox(sessionId: string) {
    return queryOne(
      `SELECT * FROM sandboxes WHERE session_id = $1 ORDER BY created_at DESC LIMIT 1`,
      [sessionId]
    );
  }

  async getSandboxWithCircuitBreaker(sessionId: string) {
    return queryOne(
      `SELECT *, spawn_failure_count, last_spawn_failure FROM sandboxes
       WHERE session_id = $1 ORDER BY created_at DESC LIMIT 1`,
      [sessionId]
    );
  }

  async createSandbox(data: CreateSandboxData) {
    const now = Date.now();
    await execute(
      `INSERT INTO sandboxes (id, session_id, sandbox_id, auth_token, status, created_at)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [data.id, data.sessionId, data.sandboxId ?? null, data.authToken ?? null, data.status ?? "pending", now]
    );
  }

  async updateSandboxStatus(sessionId: string, status: string) {
    await execute(
      `UPDATE sandboxes SET status = $1 WHERE session_id = $2
       AND id = (SELECT id FROM sandboxes WHERE session_id = $2 ORDER BY created_at DESC LIMIT 1)`,
      [status, sessionId]
    );
  }

  async updateSandboxForSpawn(sessionId: string, data: SpawnSandboxData) {
    await execute(
      `UPDATE sandboxes SET
         sandbox_id = $1, provider_object_id = $2, auth_token = $3, status = $4
       WHERE session_id = $5
       AND id = (SELECT id FROM sandboxes WHERE session_id = $5 ORDER BY created_at DESC LIMIT 1)`,
      [data.sandboxId, data.providerObjectId ?? null, data.authToken, data.status, sessionId]
    );
  }

  async updateSandboxProviderObjectId(sessionId: string, providerObjectId: string) {
    await execute(
      `UPDATE sandboxes SET provider_object_id = $1 WHERE session_id = $2
       AND id = (SELECT id FROM sandboxes WHERE session_id = $2 ORDER BY created_at DESC LIMIT 1)`,
      [providerObjectId, sessionId]
    );
  }

  async updateSandboxSnapshotImageId(sessionId: string, snapshotImageId: string) {
    await execute(
      `UPDATE sandboxes SET snapshot_image_id = $1 WHERE session_id = $2
       AND id = (SELECT id FROM sandboxes WHERE session_id = $2 ORDER BY created_at DESC LIMIT 1)`,
      [snapshotImageId, sessionId]
    );
  }

  async updateSandboxHeartbeat(sessionId: string) {
    await execute(
      `UPDATE sandboxes SET last_heartbeat = $1 WHERE session_id = $2
       AND id = (SELECT id FROM sandboxes WHERE session_id = $2 ORDER BY created_at DESC LIMIT 1)`,
      [Date.now(), sessionId]
    );
  }

  async updateSandboxLastActivity(sessionId: string) {
    await execute(
      `UPDATE sandboxes SET last_activity = $1 WHERE session_id = $2
       AND id = (SELECT id FROM sandboxes WHERE session_id = $2 ORDER BY created_at DESC LIMIT 1)`,
      [Date.now(), sessionId]
    );
  }

  async updateSandboxGitSyncStatus(sessionId: string, status: string) {
    await execute(
      `UPDATE sandboxes SET git_sync_status = $1 WHERE session_id = $2
       AND id = (SELECT id FROM sandboxes WHERE session_id = $2 ORDER BY created_at DESC LIMIT 1)`,
      [status, sessionId]
    );
  }

  async updateSandboxSpawnError(sessionId: string, error: string) {
    const now = Date.now();
    await execute(
      `UPDATE sandboxes SET last_spawn_error = $1, last_spawn_error_at = $2 WHERE session_id = $3
       AND id = (SELECT id FROM sandboxes WHERE session_id = $3 ORDER BY created_at DESC LIMIT 1)`,
      [error, now, sessionId]
    );
  }

  async incrementCircuitBreakerFailure(sessionId: string) {
    const now = Date.now();
    await execute(
      `UPDATE sandboxes SET
         spawn_failure_count = COALESCE(spawn_failure_count, 0) + 1,
         last_spawn_failure = $1
       WHERE session_id = $2
       AND id = (SELECT id FROM sandboxes WHERE session_id = $2 ORDER BY created_at DESC LIMIT 1)`,
      [now, sessionId]
    );
  }

  async resetCircuitBreaker(sessionId: string) {
    await execute(
      `UPDATE sandboxes SET spawn_failure_count = 0, last_spawn_failure = NULL
       WHERE session_id = $1
       AND id = (SELECT id FROM sandboxes WHERE session_id = $1 ORDER BY created_at DESC LIMIT 1)`,
      [sessionId]
    );
  }

  // ===== Participants =====

  async getParticipantByUserId(sessionId: string, userId: string) {
    return queryOne(
      `SELECT * FROM participants WHERE session_id = $1 AND user_id = $2`,
      [sessionId, userId]
    );
  }

  async getParticipantByWsTokenHash(sessionId: string, wsTokenHash: string) {
    return queryOne(
      `SELECT * FROM participants WHERE session_id = $1 AND ws_auth_token = $2`,
      [sessionId, wsTokenHash]
    );
  }

  async getParticipantById(participantId: string) {
    return queryOne(`SELECT * FROM participants WHERE id = $1`, [participantId]);
  }

  async createParticipant(data: CreateParticipantData) {
    const now = Date.now();
    await execute(
      `INSERT INTO participants (id, session_id, user_id, github_user_id, github_login, github_email, github_name, role, github_access_token_encrypted, github_refresh_token_encrypted, github_token_expires_at, ws_auth_token, ws_token_created_at, joined_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
      [
        data.id,
        data.sessionId,
        data.userId,
        data.githubUserId ?? null,
        data.githubLogin ?? null,
        data.githubEmail ?? null,
        data.githubName ?? null,
        data.role,
        data.githubAccessTokenEncrypted ?? null,
        data.githubRefreshTokenEncrypted ?? null,
        data.githubTokenExpiresAt ?? null,
        data.wsAuthToken ?? null,
        data.wsTokenCreatedAt ?? null,
        now,
      ]
    );
  }

  async updateParticipantTokens(
    participantId: string,
    accessTokenEncrypted: string,
    refreshTokenEncrypted?: string,
    expiresAt?: number
  ) {
    await execute(
      `UPDATE participants SET
         github_access_token_encrypted = $1,
         github_refresh_token_encrypted = COALESCE($2, github_refresh_token_encrypted),
         github_token_expires_at = COALESCE($3, github_token_expires_at)
       WHERE id = $4`,
      [accessTokenEncrypted, refreshTokenEncrypted ?? null, expiresAt ?? null, participantId]
    );
  }

  async updateParticipantWsToken(participantId: string, wsTokenHash: string) {
    await execute(
      `UPDATE participants SET ws_auth_token = $1, ws_token_created_at = $2 WHERE id = $3`,
      [wsTokenHash, Date.now(), participantId]
    );
  }

  async listParticipants(sessionId: string) {
    return query(
      `SELECT * FROM participants WHERE session_id = $1 ORDER BY joined_at`,
      [sessionId]
    );
  }

  // ===== Messages =====

  async getMessageCount(sessionId: string): Promise<number> {
    const row = await queryOne<{ count: string }>(
      `SELECT COUNT(*) as count FROM messages WHERE session_id = $1`,
      [sessionId]
    );
    return parseInt(row?.count ?? "0", 10);
  }

  async getPendingOrProcessingCount(sessionId: string): Promise<number> {
    const row = await queryOne<{ count: string }>(
      `SELECT COUNT(*) as count FROM messages WHERE session_id = $1 AND status IN ('pending', 'processing')`,
      [sessionId]
    );
    return parseInt(row?.count ?? "0", 10);
  }

  async getProcessingMessage(sessionId: string) {
    return queryOne(
      `SELECT * FROM messages WHERE session_id = $1 AND status = 'processing' ORDER BY created_at LIMIT 1`,
      [sessionId]
    );
  }

  async getNextPendingMessage(sessionId: string) {
    return queryOne(
      `SELECT * FROM messages WHERE session_id = $1 AND status = 'pending' ORDER BY created_at LIMIT 1`,
      [sessionId]
    );
  }

  async getMessageCallbackContext(messageId: string): Promise<string | null> {
    const row = await queryOne<{ callback_context: string | null }>(
      `SELECT callback_context FROM messages WHERE id = $1`,
      [messageId]
    );
    return row?.callback_context ?? null;
  }

  async createMessage(data: CreateMessageData) {
    const now = Date.now();
    await execute(
      `INSERT INTO messages (id, session_id, author_id, content, source, model, reasoning_effort, attachments, callback_context, status, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'pending', $10)`,
      [
        data.id,
        data.sessionId,
        data.authorId,
        data.content,
        data.source,
        data.model ?? null,
        data.reasoningEffort ?? null,
        data.attachments ?? null,
        data.callbackContext ?? null,
        now,
      ]
    );
  }

  async updateMessageToProcessing(messageId: string) {
    await execute(
      `UPDATE messages SET status = 'processing', started_at = $1 WHERE id = $2`,
      [Date.now(), messageId]
    );
  }

  async updateMessageCompletion(messageId: string, status: string, errorMessage?: string) {
    await execute(
      `UPDATE messages SET status = $1, error_message = $2, completed_at = $3 WHERE id = $4`,
      [status, errorMessage ?? null, Date.now(), messageId]
    );
  }

  async listMessages(sessionId: string, limit = 50, offset = 0) {
    return query(
      `SELECT * FROM messages WHERE session_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3`,
      [sessionId, limit, offset]
    );
  }

  // ===== Events =====

  async createEvent(data: CreateEventData) {
    const now = Date.now();
    await execute(
      `INSERT INTO events (id, session_id, type, data, message_id, created_at)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [data.id, data.sessionId, data.type, data.data, data.messageId ?? null, now]
    );
  }

  async listEvents(options: ListEventsOptions) {
    const conditions: string[] = [`session_id = $1`];
    const params: unknown[] = [options.sessionId];
    let idx = 2;

    if (options.afterTimestamp && options.afterId) {
      conditions.push(`(created_at > $${idx} OR (created_at = $${idx} AND id > $${idx + 1}))`);
      params.push(options.afterTimestamp, options.afterId);
      idx += 2;
    }

    if (options.beforeTimestamp && options.beforeId) {
      conditions.push(`(created_at < $${idx} OR (created_at = $${idx} AND id < $${idx + 1}))`);
      params.push(options.beforeTimestamp, options.beforeId);
      idx += 2;
    }

    const limit = options.limit ?? 100;

    return query(
      `SELECT * FROM events
       WHERE ${conditions.join(" AND ")}
       ORDER BY created_at ASC, id ASC
       LIMIT $${idx}`,
      [...params, limit]
    );
  }

  async getEventsForReplay(sessionId: string, limit = 500) {
    return query(
      `SELECT * FROM events WHERE session_id = $1 ORDER BY created_at DESC, id DESC LIMIT $2`,
      [sessionId, limit]
    );
  }

  async getEventsHistoryPage(
    sessionId: string,
    beforeTimestamp: number,
    beforeId: string,
    limit = 100
  ) {
    return query(
      `SELECT * FROM events
       WHERE session_id = $1 AND (created_at < $2 OR (created_at = $2 AND id < $3))
       ORDER BY created_at DESC, id DESC
       LIMIT $4`,
      [sessionId, beforeTimestamp, beforeId, limit]
    );
  }

  // ===== Artifacts =====

  async createArtifact(data: CreateArtifactData) {
    const now = Date.now();
    await execute(
      `INSERT INTO artifacts (id, session_id, type, url, metadata, created_at)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [data.id, data.sessionId, data.type, data.url ?? null, data.metadata ?? null, now]
    );
  }

  async listArtifacts(sessionId: string) {
    return query(
      `SELECT * FROM artifacts WHERE session_id = $1 ORDER BY created_at`,
      [sessionId]
    );
  }

  // ===== WS Client Mapping =====

  async upsertWsClientMapping(sessionId: string, wsId: string, participantId: string, clientId?: string) {
    const now = Date.now();
    await execute(
      `INSERT INTO ws_client_mapping (ws_id, session_id, participant_id, client_id, created_at)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (ws_id) DO UPDATE SET
         participant_id = EXCLUDED.participant_id,
         client_id = EXCLUDED.client_id`,
      [wsId, sessionId, participantId, clientId ?? null, now]
    );
  }

  async getWsClientMapping(wsId: string) {
    return queryOne(
      `SELECT * FROM ws_client_mapping WHERE ws_id = $1`,
      [wsId]
    );
  }

  async deleteWsClientMapping(wsId: string) {
    await execute(`DELETE FROM ws_client_mapping WHERE ws_id = $1`, [wsId]);
  }

  // ===== PR Helper =====

  async getProcessingMessageAuthor(sessionId: string) {
    return queryOne(
      `SELECT p.* FROM participants p
       JOIN messages m ON m.author_id = p.id
       WHERE m.session_id = $1 AND m.status = 'processing'
       ORDER BY m.created_at DESC LIMIT 1`,
      [sessionId]
    );
  }
}
