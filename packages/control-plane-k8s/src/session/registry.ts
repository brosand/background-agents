/**
 * SessionActorRegistry — manages in-process SessionActor instances.
 *
 * In the Cloudflare version, the Durable Object runtime handled actor
 * addressing (env.SESSION.idFromName(id) → stub.get(id)). Here we
 * maintain an in-memory registry that creates actors on demand.
 *
 * For a production deployment with Rivet Engine, this would be replaced
 * by Rivet's built-in actor routing (getOrCreate). This registry is a
 * lightweight standalone alternative for dev and simple deployments.
 *
 * Key guarantees:
 * - Single instance per session ID within this process
 * - Actor is created on first access (lazy initialization)
 * - Idle actors can be evicted (not yet implemented — Rivet handles this)
 */

import type pg from "pg";
import { SessionActor } from "./actor.js";
import type { Config } from "../utils/config.js";
import { createLogger } from "../utils/logger.js";

const log = createLogger("session-registry");

export class SessionActorRegistry {
  private readonly actors = new Map<string, SessionActor>();

  constructor(
    private readonly pool: pg.Pool,
    private readonly config: Config
  ) {}

  /**
   * Get or create a SessionActor for the given session ID.
   */
  getOrCreate(sessionId: string): SessionActor {
    let actor = this.actors.get(sessionId);
    if (!actor) {
      actor = new SessionActor(sessionId, this.pool, this.config);
      this.actors.set(sessionId, actor);
      log.info("Created session actor", { session_id: sessionId, active_actors: this.actors.size });
    }
    return actor;
  }

  /**
   * Check if an actor exists for the given session ID.
   */
  has(sessionId: string): boolean {
    return this.actors.has(sessionId);
  }

  /**
   * Remove and destroy an actor.
   */
  evict(sessionId: string): void {
    const actor = this.actors.get(sessionId);
    if (actor) {
      actor.destroy();
      this.actors.delete(sessionId);
      log.info("Evicted session actor", { session_id: sessionId, active_actors: this.actors.size });
    }
  }

  /**
   * Get the count of active actors.
   */
  get size(): number {
    return this.actors.size;
  }

  /**
   * Destroy all actors (for graceful shutdown).
   */
  destroyAll(): void {
    for (const [id, actor] of this.actors) {
      actor.destroy();
      log.info("Destroyed session actor", { session_id: id });
    }
    this.actors.clear();
  }
}
