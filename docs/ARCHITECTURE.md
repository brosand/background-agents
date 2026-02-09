# Open-Inspect Architecture (Rivet + Kubernetes)

## Overview

Open-Inspect has been rebuilt from Cloudflare Workers + Modal to a Kubernetes-native architecture
using Rivet actor patterns for session management. This eliminates vendor lock-in and allows
deployment on any Kubernetes cluster.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────┐
│ GitHub (Source Control)                                  │
│ - Repos being worked on                                 │
│ - CI/CD via GitHub Actions                              │
└─────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│ Kubernetes Cluster                                       │
│                                                          │
│  ┌──────────────────┐  ┌──────────────────┐             │
│  │  Web App (Next.js)│  │ Control Plane     │             │
│  │  packages/web     │  │ (Hono + Rivet     │             │
│  │  Port 3000        │  │  Actor patterns)  │             │
│  └────────┬─────────┘  │  Port 8787        │             │
│           │             └────────┬─────────┘             │
│           │ HTTP/WS              │                        │
│           └──────────────────────┘                        │
│                         │                                 │
│           ┌─────────────┼─────────────┐                  │
│           ▼             ▼             ▼                   │
│  ┌──────────────┐ ┌──────────┐ ┌──────────┐            │
│  │ PostgreSQL   │ │  Redis   │ │Sandbox API│            │
│  │ (All state)  │ │ (Cache)  │ │(Pod mgmt) │            │
│  └──────────────┘ └──────────┘ └─────┬─────┘            │
│                                       │                   │
│                                       ▼                   │
│                          ┌────────────────────┐          │
│                          │  Sandbox Pods       │          │
│                          │  (open-inspect-     │          │
│                          │   sandboxes ns)     │          │
│                          │  - OpenCode CLI     │          │
│                          │  - Bridge to CP     │          │
│                          │  - Dev tools        │          │
│                          └────────────────────┘          │
└─────────────────────────────────────────────────────────┘
```

## Component Mapping (Old → New)

| Old (Cloudflare + Modal)     | New (K8s + Rivet)                          |
|------------------------------|--------------------------------------------|
| Cloudflare Worker            | Hono HTTP server (`packages/control-plane`) |
| Durable Objects              | Rivet-style session actors (in-process)     |
| D1 Database                  | PostgreSQL                                  |
| KV Namespace (cache)         | Redis                                       |
| Modal Sandbox                | K8s Pod with PVC workspace                  |
| Modal Image                  | Docker image (`packages/sandbox-image`)     |
| Modal `snapshot_filesystem`  | K8s VolumeSnapshot (CSI)                    |
| Modal Web API                | FastAPI service (`packages/sandbox-api`)    |
| Terraform (CF/Modal/Vercel)  | K8s manifests (`k8s/`)                      |
| Vercel (web app)             | K8s Deployment (or keep on Vercel)          |

## Packages

### `packages/control-plane/`

The brain of the system. Manages sessions, WebSocket connections, and sandbox lifecycle.

**Key files (new):**
- `src/server.ts` — Hono HTTP server entry point (replaces `src/index.ts`)
- `src/actors/session-actor-manager.ts` — Per-session state management (replaces Durable Objects)
- `src/db/postgres.ts` — PostgreSQL client abstraction
- `src/db/schema.sql` — Full database schema
- `src/db/migrate.ts` — Migration runner
- `src/db/pg-session-index.ts` — Session listing (replaces D1 `session-index.ts`)
- `src/db/pg-repo-metadata.ts` — Repo metadata (replaces D1 `repo-metadata.ts`)
- `src/db/pg-repo-secrets.ts` — Encrypted repo secrets (replaces D1 `repo-secrets.ts`)
- `src/session/pg-repository.ts` — Session data access (replaces DO `repository.ts`)
- `src/sandbox/providers/k8s-provider.ts` — K8s sandbox provider (replaces `modal-provider.ts`)
- `src/cache/redis.ts` — Redis cache (replaces KV)

**Preserved files (infrastructure-agnostic):**
- `src/sandbox/provider.ts` — Provider interface (unchanged)
- `src/sandbox/lifecycle/` — Lifecycle state machine (unchanged)
- `src/auth/` — Authentication utilities (unchanged)
- `src/source-control/` — GitHub integration (unchanged)
- `src/logger.ts` — Logging (unchanged)

### `packages/sandbox-api/`

**NEW.** FastAPI service that manages K8s sandbox pods. Replaces Modal's web API endpoints.

- `src/api.py` — FastAPI routes (create, warm, snapshot, restore, terminate)
- `src/k8s_manager.py` — K8s pod lifecycle management
- `src/auth/` — HMAC authentication (portable from Modal)

### `packages/sandbox-image/`

**NEW.** Dockerfile for building sandbox container images. Replaces Modal's image builder
(`packages/modal-infra/src/images/base.py`).

### `packages/web/`

Next.js frontend — **unchanged**. The web app communicates with the control plane via
`CONTROL_PLANE_URL` and WebSocket URLs, which are infrastructure-agnostic.

### `packages/shared/`

Shared TypeScript types and utilities — **unchanged**.

### `packages/modal-infra/` (legacy)

The original Modal infrastructure code. Kept for reference but no longer deployed.
The sandbox entrypoint, bridge, and types from this package are reused in the sandbox image.

## Database

All state is stored in a single PostgreSQL database. The schema (`packages/control-plane/src/db/schema.sql`)
consolidates:

1. **D1 session index** → `sessions` table (with `owner_user_id` column)
2. **D1 repo_metadata** → `repo_metadata` table
3. **D1 repo_secrets** → `repo_secrets` table
4. **Per-DO session SQLite** → `sessions`, `participants`, `messages`, `events`, `artifacts`, `sandboxes` tables
   (all scoped by `session_id` foreign keys)

Key changes from the DO SQLite model:
- All tables have `session_id` foreign keys (was implicit in DO)
- `modal_sandbox_id` → `sandbox_id` (generic)
- `modal_object_id` → `provider_object_id` (generic)
- `snapshot_image_id` now stores VolumeSnapshot names instead of Modal Image IDs

## Sandbox Lifecycle

```
1. User sends prompt
   ↓
2. Control plane creates message in PostgreSQL
   ↓
3. Session actor checks sandbox status
   ↓
4. If no sandbox: POST /api/create-sandbox to sandbox-api
   ↓
5. Sandbox-api creates K8s Pod + PVC in open-inspect-sandboxes namespace
   ↓
6. Pod runs sandbox image:
   - PID 1 supervisor (entrypoint.py)
   - Git clone/sync
   - OpenCode server
   - Bridge to control plane (WebSocket)
   ↓
7. Bridge connects back to control plane
   ↓
8. Events stream: tool calls, tokens, git sync, execution complete
   ↓
9. On inactivity (10 min default):
   - VolumeSnapshot taken via sandbox-api
   - Pod terminated
   ↓
10. On next prompt:
    - New Pod created from VolumeSnapshot (fast restore)
    - Quick git pull, ready in seconds
```

## Deployment

### Quick Start (Development)

```bash
# 1. Start local K8s cluster (e.g., minikube, k3d, kind)
k3d cluster create open-inspect

# 2. Build images
docker build -t open-inspect-sandbox:latest packages/sandbox-image/
docker build -t open-inspect-sandbox-api:latest packages/sandbox-api/
docker build -t open-inspect-control-plane:latest -f packages/control-plane/Dockerfile .
docker build -t open-inspect-web:latest packages/web/

# 3. Load images into cluster
k3d image import open-inspect-sandbox:latest open-inspect-sandbox-api:latest \
  open-inspect-control-plane:latest open-inspect-web:latest

# 4. Update secrets in k8s/base/secrets.yaml

# 5. Deploy
kubectl apply -k k8s/
```

### Production

See `.github/workflows/deploy-k8s.yml` for the CI/CD pipeline that:
1. Builds and pushes images to GHCR
2. Updates K8s manifests with SHA-tagged images
3. Applies to the cluster
4. Verifies health

## Configuration

All configuration is via environment variables and K8s secrets:

| Variable | Service | Description |
|----------|---------|-------------|
| `DATABASE_URL` | Control Plane | PostgreSQL connection string |
| `REDIS_URL` | Control Plane | Redis connection string |
| `SANDBOX_API_URL` | Control Plane | URL of the sandbox-api service |
| `API_SECRET` | Both | HMAC shared secret for internal auth |
| `TOKEN_ENCRYPTION_KEY` | Control Plane | AES-256 key for encrypting tokens |
| `REPO_SECRETS_ENCRYPTION_KEY` | Control Plane | AES-256 key for repo secrets |
| `GITHUB_APP_*` | Both | GitHub App credentials |
| `SANDBOX_NAMESPACE` | Sandbox API | K8s namespace for sandbox pods |
| `SANDBOX_IMAGE` | Sandbox API | Docker image for sandbox pods |
