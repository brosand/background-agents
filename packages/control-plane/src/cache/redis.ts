/**
 * Redis cache.
 *
 * Replaces Cloudflare KV for caching (repos list, etc.).
 * Uses ioredis for K8s-native Redis connectivity.
 */

import Redis from "ioredis";
import { createLogger } from "../logger";

const logger = createLogger("redis-cache");

export class RedisCache {
  private client: Redis;

  constructor(url: string) {
    this.client = new Redis(url, {
      maxRetriesPerRequest: 3,
      retryStrategy(times) {
        return Math.min(times * 200, 3000);
      },
      lazyConnect: true,
    });

    this.client.on("error", (err) => {
      logger.error("Redis error", { error: err.message });
    });

    this.client.connect().catch((err) => {
      logger.warn("Redis connection failed - caching disabled", { error: err.message });
    });
  }

  /**
   * Get a cached value.
   */
  async get<T = unknown>(key: string): Promise<T | null> {
    try {
      const data = await this.client.get(key);
      if (!data) return null;
      return JSON.parse(data) as T;
    } catch {
      return null;
    }
  }

  /**
   * Set a cached value with TTL in seconds.
   */
  async set(key: string, value: unknown, ttlSeconds: number): Promise<void> {
    try {
      await this.client.set(key, JSON.stringify(value), "EX", ttlSeconds);
    } catch {
      // Cache write failure is non-fatal
    }
  }

  /**
   * Delete a cached key.
   */
  async del(key: string): Promise<void> {
    try {
      await this.client.del(key);
    } catch {
      // Cache delete failure is non-fatal
    }
  }

  /**
   * Shutdown the Redis connection.
   */
  async shutdown(): Promise<void> {
    await this.client.quit();
  }
}
