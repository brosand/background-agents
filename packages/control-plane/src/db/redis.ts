/**
 * Redis cache for Open-Inspect Control Plane.
 *
 * Replaces Cloudflare KV namespace with Redis for caching.
 */

import { createClient, type RedisClientType } from "redis";
import { createLogger } from "../logger";

const logger = createLogger("redis");

export class RedisCache {
  private client: RedisClientType;
  private connected = false;
  private prefix: string;

  constructor(url: string, prefix = "oi:") {
    this.prefix = prefix;
    this.client = createClient({ url }) as RedisClientType;

    this.client.on("error", (err: Error) => {
      logger.error("Redis client error", { error: err });
    });

    this.client.on("connect", () => {
      this.connected = true;
      logger.info("Redis connected");
    });

    this.client.on("end", () => {
      this.connected = false;
    });
  }

  async connect(): Promise<void> {
    if (!this.connected) {
      await this.client.connect();
    }
  }

  async disconnect(): Promise<void> {
    if (this.connected) {
      await this.client.quit();
    }
  }

  /**
   * Get a value from cache.
   */
  async get<T>(key: string): Promise<T | null> {
    try {
      const value = await this.client.get(this.prefix + key);
      if (!value) return null;
      return JSON.parse(value) as T;
    } catch (err) {
      logger.warn("Redis get failed", { key, error: err instanceof Error ? err : String(err) });
      return null;
    }
  }

  /**
   * Set a value in cache with optional TTL in seconds.
   */
  async put(key: string, value: unknown, ttlSeconds?: number): Promise<void> {
    try {
      const serialized = JSON.stringify(value);
      if (ttlSeconds) {
        await this.client.setEx(this.prefix + key, ttlSeconds, serialized);
      } else {
        await this.client.set(this.prefix + key, serialized);
      }
    } catch (err) {
      logger.warn("Redis put failed", { key, error: err instanceof Error ? err : String(err) });
    }
  }

  /**
   * Delete a value from cache.
   */
  async delete(key: string): Promise<void> {
    try {
      await this.client.del(this.prefix + key);
    } catch (err) {
      logger.warn("Redis delete failed", { key, error: err instanceof Error ? err : String(err) });
    }
  }
}
