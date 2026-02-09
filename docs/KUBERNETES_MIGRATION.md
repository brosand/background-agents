# Migrating the Control Plane from Cloudflare to Kubernetes

This document explores options for running the Open-Inspect control plane on Kubernetes instead of
Cloudflare Workers, evaluating Rivet, RivetKit, and a roll-your-own approach.

## Current Cloudflare Dependencies

The control plane relies on five Cloudflare-specific services:

| Service | What it does in our codebase |
|---------|------------------------------|
| **Workers** | HTTP API gateway, WebSocket upgrade routing (`src/index.ts`, `src/router.ts`) |
| **Durable Objects (DO)** | Per-session stateful actor with embedded SQLite — messages, events, artifacts, sandbox lifecycle (`src/session/durable-object.ts`, 2700+ lines) |
| **DO SQLite** | Schema-managed relational storage inside each DO (`src/session/schema.ts`, `src/session/repository.ts`) |
| **DO WebSockets + Hibernation** | Bidirectional real-time communication with tagged WebSocket recovery after hibernation (`src/session/websocket-manager.ts`) |
| **DO Alarms** | Scheduled inactivity checks, timeouts (`ctx.storage.setAlarm()`) |
| **D1 Database** | Shared relational data — session index, repo metadata, encrypted repo secrets (`src/db/`) |
| **KV Namespace** | Stale-while-revalidate cache for repository list (`REPOS_CACHE`) |
| **`ctx.waitUntil()`** | Background work after response (cache refresh, Slack notifications, snapshots) |

The hardest pieces to replace are **Durable Objects** (stateful actors with co-located SQLite,
WebSocket hibernation, and alarms) and the **WebSocket hibernation model** (tagged sockets that
survive process sleep/wake cycles).

---

## Option 1: Rivet Actors (Recommended)

[Rivet](https://rivet.dev) is the closest open-source analog to Cloudflare Durable Objects. It
provides long-lived, stateful "Actors" with built-in WebSocket support, hibernation, state
persistence, and automatic scaling. Apache 2.0 licensed.

### Architecture

```
                    ┌─────────────────────────────────┐
                    │         Kubernetes Cluster       │
                    │                                  │
  HTTP/WS ────────►│  Ingress (nginx / traefik)       │
                    │         │                        │
                    │         ▼                        │
                    │  ┌─────────────┐                 │
                    │  │  API Server │ (Express/Hono)  │
                    │  │  (router)   │                 │
                    │  └──────┬──────┘                 │
                    │         │ RPC / getOrCreate()    │
                    │         ▼                        │
                    │  ┌─────────────────────┐         │
                    │  │   Rivet Engine       │        │
                    │  │  (actor orchestrator)│        │
                    │  └──────┬──────────────┘         │
                    │         │                        │
                    │    ┌────┴────┐                   │
                    │    ▼         ▼                   │
                    │ [Session   [Session              │
                    │  Actor A]   Actor B]  ...        │
                    │    │         │                   │
                    │    ▼         ▼                   │
                    │  PostgreSQL / FoundationDB       │
                    │  (actor state persistence)       │
                    └─────────────────────────────────┘
```

### What maps directly

| Cloudflare concept | Rivet equivalent |
|--------------------|------------------|
| Durable Object class | Actor class (extends `Actor`) |
| `ctx.storage.sql` (DO SQLite) | Actor state (`this._state`) — in-memory with persistence; or use an external DB per actor |
| `ctx.acceptWebSocket(ws, tags)` | Built-in WebSocket support with hibernation; connections survive actor sleep/wake |
| `ctx.storage.setAlarm(ts)` | `setTimeout` / `setInterval` inside actors (they're long-lived processes) |
| `ctx.getWebSockets()` | Rivet maintains WS connections across hibernation natively |
| `ctx.waitUntil()` | Not needed — actors are long-lived, just do async work |
| DO ID routing | `getOrCreate(id)` — Rivet routes to the correct actor by ID |
| Global uniqueness | Single-instance guarantee per actor ID |

### What doesn't map 1:1

| Gap | Mitigation |
|-----|------------|
| **DO SQLite** — each DO has its own embedded SQL database | Rivet actors have in-memory state with persistence, but not SQL. Use PostgreSQL (one DB per session, or a shared DB with session-scoped tables/schemas) or SQLite via better-sqlite3 in the actor process. |
| **D1** — shared relational database | Replace with PostgreSQL on K8s (via operator like CloudNativePG) or a managed DB (RDS, Cloud SQL). |
| **KV** — simple key-value cache with TTL | Replace with Redis on K8s (Bitnami Helm chart or managed Redis). |
| **WebSocket hibernation tags** | Rivet supports WebSocket hibernation natively. Tag-based classification would need to be reimplemented in application code (a Map of wsId → metadata). |
| **Wrangler / Terraform CF provider** | Standard Kubernetes manifests / Helm charts. Rivet Engine deploys as a single binary or Docker container. |

### Deployment on Kubernetes

Rivet has [official Kubernetes documentation](https://www.rivet.dev/docs/connect/kubernetes/). The
setup involves:

1. **Deploy Rivet Engine** as a Deployment + Service (single Rust binary / Docker image)
2. **Configure backing store** — PostgreSQL (recommended for K8s) or FoundationDB
3. **Deploy your app** — a container running your RivetKit-based TypeScript backend
4. **Connect via environment variables** — `RIVET_ENDPOINT` and `RIVET_PUBLIC_ENDPOINT`
5. **Expose via Ingress** — standard nginx/traefik ingress for HTTP + WebSocket upgrade

### Migration approach (RivetKit)

[RivetKit](https://www.rivet.dev/changelog/2025-07-01-introducing-rivetkit-backend-libraries-that-replace-saas/)
is a portable TypeScript library that can run standalone (no Rivet Engine needed for dev/simple
deployments). This provides an incremental migration path:

1. **Phase 1**: Rewrite `SessionDO` as a Rivet Actor class using RivetKit
   - Port the SQLite schema to PostgreSQL (or embed SQLite in the actor)
   - Replace `ctx.acceptWebSocket()` with Rivet's WebSocket primitives
   - Replace `ctx.storage.setAlarm()` with `setTimeout`/`setInterval`
   - Replace `ctx.waitUntil()` with plain `async` calls (actors are long-lived)
2. **Phase 2**: Replace D1 stores with PostgreSQL repositories
3. **Phase 3**: Replace KV cache with Redis
4. **Phase 4**: Replace Cloudflare router with Express/Hono HTTP server
5. **Phase 5**: Deploy on K8s with Rivet Engine for production orchestration

### Estimated effort

The `SessionDO` is ~2,700 lines and deeply coupled to Cloudflare APIs. The rewrite is substantial
but mostly mechanical — the business logic (message handling, lifecycle management, event storage)
stays the same. The Cloudflare-specific surface area is:

- ~15 uses of `ctx.acceptWebSocket` / `ctx.getWebSockets` / `ctx.getTags`
- ~10 uses of `ctx.storage.sql` (SQLite queries)
- ~8 uses of `ctx.storage.setAlarm`
- ~12 uses of `ctx.waitUntil`
- ~5 uses of `WebSocketPair` / `WebSocketRequestResponsePair`

---

## Option 2: Roll Your Own on Kubernetes

If you want to avoid taking a dependency on Rivet, you can implement the actor model yourself using
Kubernetes primitives + standard databases.

### Architecture

```
                    ┌─────────────────────────────────┐
                    │         Kubernetes Cluster       │
                    │                                  │
  HTTP/WS ────────►│  Ingress                         │
                    │    │                             │
                    │    ▼                             │
                    │  ┌──────────────┐                │
                    │  │ API Gateway  │ (Hono/Express) │
                    │  │ + WS Proxy   │                │
                    │  └──────┬───────┘                │
                    │         │                        │
                    │    ┌────┴────┐                   │
                    │    ▼         ▼                   │
                    │  Pod A     Pod B    (session     │
                    │  (sessions (sessions  workers)   │
                    │   1-100)   101-200)              │
                    │    │         │                   │
                    │    ▼         ▼                   │
                    │  ┌─────────────────┐             │
                    │  │   PostgreSQL    │             │
                    │  │   + Redis      │             │
                    │  └─────────────────┘             │
                    └─────────────────────────────────┘
```

### Component mapping

| Cloudflare service | Kubernetes replacement |
|--------------------|----------------------|
| **Workers** | Deployment running Hono/Express HTTP server |
| **Durable Objects** | In-process actor registry with consistent hashing; or one-pod-per-session via a custom operator |
| **DO SQLite** | PostgreSQL with session-scoped rows, or embedded SQLite per pod |
| **DO WebSockets** | `ws` library in Node.js; sticky sessions via ingress annotation (`nginx.ingress.kubernetes.io/affinity: cookie`) |
| **DO Alarms** | Bull/BullMQ job scheduler on Redis, or `node-cron` in-process |
| **D1** | PostgreSQL (shared tables) |
| **KV** | Redis with TTL (`SET key value EX 300`) |
| **`ctx.waitUntil()`** | Fire-and-forget promises or a background job queue |

### Key challenges

1. **Single-instance guarantee** — Durable Objects guarantee exactly one instance per ID globally.
   On K8s, you need either:
   - **Consistent hashing** (e.g., hash ring) to route session IDs to specific pods, with leader
     election for failover
   - **Distributed lock** (Redis `SETNX` or PostgreSQL advisory locks) before processing a session
   - **StatefulSet with 1 replica per session** — doesn't scale well

2. **WebSocket affinity** — WebSocket connections must route to the pod owning that session. Options:
   - Sticky sessions via ingress (cookie/IP-based)
   - A dedicated WebSocket gateway that maintains a session→pod routing table in Redis

3. **Hibernation** — Cloudflare hibernates idle DOs to save resources. On K8s you'd need to
   implement your own idle detection + scale-to-zero (KEDA can help) or just accept always-on pods.

4. **State persistence on crash** — DOs persist SQLite state across restarts. On K8s, you need
   external persistence (PostgreSQL) since pod-local storage is ephemeral.

### Estimated effort

Significantly more than the Rivet approach. You're essentially building your own actor framework.
The WebSocket routing, single-instance guarantees, and hibernation are the hardest parts.

---

## Option 3: Temporal + Standard K8s Services

[Temporal](https://temporal.io) is a workflow orchestration engine that provides durable execution.
It's not a direct replacement for Durable Objects but can handle the lifecycle/state machine aspects.

### What it solves

- Durable execution with automatic retries and state persistence
- Timer-based scheduling (replaces DO alarms)
- Activity-based side effects

### What it doesn't solve

- Real-time WebSocket management (you still need a separate WS server)
- Per-session in-memory state (Temporal workflows are event-sourced, not in-memory actors)
- The latency profile is different — Temporal adds overhead for durability

### When to consider

Temporal makes sense if the control plane evolves toward long-running workflows (multi-step
deployments, approval chains) rather than real-time session coordination. For the current
architecture, which is heavily WebSocket-driven, it's not an ideal fit.

---

## Comparison Summary

| Criteria | Rivet | Roll Your Own | Temporal |
|----------|-------|---------------|----------|
| **Closest to current architecture** | Yes — actors with WS, hibernation, state | Partial — you build the actor layer | No — different paradigm |
| **Migration effort** | Medium — rewrite DO class, swap DB layer | High — build actor framework + infra | High — rearchitect around workflows |
| **WebSocket support** | Native with hibernation | Manual (ws + sticky sessions + routing) | Separate service needed |
| **Single-instance guarantee** | Built-in | Must implement (locks, hashing) | Built-in (workflow ID) |
| **Hibernation / scale-to-zero** | Built-in | Manual (KEDA or custom) | N/A (workers always running) |
| **Operational complexity** | Low — single binary + PostgreSQL | High — many moving parts | Medium — Temporal cluster + workers |
| **Maturity** | Growing (production at scale for games) | Depends on your team | Very mature |
| **Vendor lock-in** | Apache 2.0, self-hostable | None | Apache 2.0, self-hostable |

---

## Recommendation

**Use Rivet Actors** for the migration. The rationale:

1. **Closest conceptual match** — Rivet's actor model was explicitly designed as a Durable Objects
   alternative. The mental model (single-instance actors with state, WebSockets, hibernation)
   transfers directly.

2. **Incremental path via RivetKit** — Start with RivetKit as a standalone library during
   development, then add Rivet Engine for production orchestration on K8s.

3. **Kubernetes-native** — Official K8s deployment support with standard Helm/manifests.

4. **Avoids building an actor framework** — The roll-your-own approach requires solving
   single-instance routing, WebSocket affinity, hibernation, and state persistence from scratch.

5. **Open source** — Apache 2.0, no vendor lock-in. Can fork if needed.

### Suggested migration order

```
1. Set up PostgreSQL on K8s (CloudNativePG operator)
   └── Migrate D1 tables (session_index, repo_metadata, repo_secrets)

2. Set up Redis on K8s
   └── Replace KV namespace (REPOS_CACHE)

3. Build HTTP API server (Hono or Express)
   └── Port src/router.ts routes (drop Cloudflare Env bindings)

4. Rewrite SessionDO as a Rivet Actor
   └── Port SQLite schema to PostgreSQL (or embedded SQLite)
   └── Port WebSocket handling to Rivet WS primitives
   └── Replace alarms with timers
   └── Replace ctx.waitUntil with async calls

5. Deploy Rivet Engine + app on K8s
   └── Helm chart with Ingress, PostgreSQL, Redis, Rivet Engine

6. Update Terraform to provision K8s resources instead of Cloudflare
```

---

## References

- [Rivet — Stateful Backends](https://www.rivet.dev/)
- [Rivet Actors vs Cloudflare Durable Objects](https://www.rivet.dev/rivet-vs-cloudflare-workers/)
- [Rivet Kubernetes Deployment](https://www.rivet.dev/docs/connect/kubernetes/)
- [RivetKit — Backend Libraries That Replace SaaS](https://www.rivet.dev/changelog/2025-07-01-introducing-rivetkit-backend-libraries-that-replace-saas/)
- [Rivet Engine (GitHub)](https://github.com/rivet-dev/engine)
- [Rivet Actors (GitHub)](https://github.com/rivet-dev/rivet)
- [Cloudflare Durable Objects Docs](https://developers.cloudflare.com/durable-objects/)
- [Vorker — Self-hosted Workers alternative](https://github.com/VaalaCat/vorker)
- [Northflank — Cloudflare Workers Alternatives](https://northflank.com/blog/best-cloudflare-workers-alternatives)
