/**
 * Redis cache — replaces Cloudflare KV Namespace (REPOS_CACHE).
 *
 * Implements the same stale-while-revalidate caching pattern used in
 * the Cloudflare version, but backed by Redis instead of KV.
 */

import Redis from "ioredis";

let redis: Redis | null = null;

export function getRedis(): Redis {
  if (!redis) {
    redis = new Redis(process.env.REDIS_URL || "redis://localhost:6379", {
      maxRetriesPerRequest: 3,
      lazyConnect: true,
    });
  }
  return redis;
}

export async function closeRedis(): Promise<void> {
  if (redis) {
    await redis.quit();
    redis = null;
  }
}

// Cache keys
const REPOS_CACHE_KEY = "repos:list";
const REPOS_CACHE_FRESH_SECONDS = 5 * 60;       // Fresh for 5 min
const REPOS_CACHE_TTL_SECONDS = 60 * 60;         // Keep in Redis for 1 hour

export interface CachedReposList {
  repos: unknown[];
  cachedAt: number;
  freshUntil: number;
}

export class ReposCacheStore {
  constructor(private readonly redis: Redis) {}

  async get(): Promise<CachedReposList | null> {
    const raw = await this.redis.get(REPOS_CACHE_KEY);
    if (!raw) return null;

    try {
      return JSON.parse(raw) as CachedReposList;
    } catch {
      return null;
    }
  }

  async set(repos: unknown[]): Promise<void> {
    const now = Date.now();
    const cached: CachedReposList = {
      repos,
      cachedAt: now,
      freshUntil: now + REPOS_CACHE_FRESH_SECONDS * 1000,
    };

    await this.redis.setex(
      REPOS_CACHE_KEY,
      REPOS_CACHE_TTL_SECONDS,
      JSON.stringify(cached)
    );
  }

  async invalidate(): Promise<void> {
    await this.redis.del(REPOS_CACHE_KEY);
  }

  isFresh(cached: CachedReposList): boolean {
    return Date.now() < cached.freshUntil;
  }
}
