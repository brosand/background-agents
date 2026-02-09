# Kubernetes-Native Architecture: Clean-Slate Design

How to rebuild Open-Inspect on Kubernetes using `agent-sandbox` CRDs, Rivet Sandbox Agent SDK,
RivetKit with Rivet Engine, PostgreSQL, NATS, and Redis.

## The Four Tools and What They Replace

| Tool | Replaces | Layer |
|------|----------|-------|
| **[Rivet Engine](https://github.com/rivet-dev/rivet)** | Cloudflare Workers runtime + Durable Object scheduling | Actor orchestration |
| **[RivetKit](https://github.com/rivet-dev/rivetkit)** | Durable Object class (`SessionDO`) | Actor framework |
| **[`kubernetes-sigs/agent-sandbox`](https://github.com/kubernetes-sigs/agent-sandbox)** | Modal (container lifecycle, warm pools, snapshots) | Sandbox infrastructure |
| **[Rivet Sandbox Agent SDK](https://github.com/rivet-dev/sandbox-agent)** | `bridge.py` + OpenCode-specific integration | Agent abstraction |

## Why Rivet Engine Matters

Previously we said "skip Rivet Engine, K8s handles scheduling." That was wrong. Looking at Rivet's
[actual K8s manifests](https://github.com/rivet-dev/rivet/tree/main/k8s/engine), Engine provides
things K8s Deployments alone cannot:

1. **Actor-level routing** — routes requests to the specific process holding an actor's in-memory
   state. K8s Services do round-robin load balancing; Engine routes by actor ID.
2. **Actor lifecycle** — hibernates idle actors to free memory, wakes them instantly on request.
   Without Engine, all actors stay in memory forever or you build this yourself.
3. **State co-location** — keeps actor state on the same machine as compute for fast reads/writes.
   Without Engine, every state access is a PostgreSQL round-trip.
4. **Multi-datacenter topology** — built-in peer discovery and leader election across regions.
5. **HPA-aware scaling** — scales 2-10 replicas based on CPU (60%) and memory (80%), with actors
   rebalanced across replicas.

Engine is a single Rust binary (`rivetkit/engine` Docker image) that sits between your app and
PostgreSQL + NATS. Your RivetKit actors connect to it and it handles the rest.

---

## Architecture

```
                            ┌──────────────────────────────┐
                            │       Web App / Slack         │
                            └─────────────┬────────────────┘
                                          │ HTTP + WebSocket
                                          ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  K8s Namespace: open-inspect                                             │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │  App Deployment (your Node.js process)                              │  │
│  │                                                                     │  │
│  │  ┌──────────────┐  ┌────────────────────────┐  ┌────────────────┐  │  │
│  │  │ Hono HTTP    │  │ RivetKit SessionActor  │  │ K8s Sandbox    │  │  │
│  │  │ router       │──│ (connects to Engine)   │──│ Provider       │  │  │
│  │  │              │  │                        │  │ (creates CRs)  │  │  │
│  │  │ /sessions    │  │ State lives in-memory, │  │                │  │  │
│  │  │ /repos       │  │ persisted by Engine    │  │ kubectl API    │  │  │
│  │  │ /health      │  │ to PostgreSQL          │  │                │  │  │
│  │  └──────────────┘  └────────────┬───────────┘  └───────┬────────┘  │  │
│  │                                 │                      │           │  │
│  └─────────────────────────────────┼──────────────────────┼───────────┘  │
│                                    │                      │              │
│  ┌─────────────────────────────────▼──────────────────┐   │              │
│  │  Rivet Engine (Deployment, 2-10 replicas via HPA)  │   │              │
│  │                                                     │   │              │
│  │  rivetkit/engine:latest                             │   │              │
│  │  ├── port 6420 (guard — actor routing)              │   │              │
│  │  ├── port 6421 (api-peer — cluster internal)        │   │              │
│  │  ├── config: /etc/rivet/config.jsonc                │   │              │
│  │  │   {                                              │   │              │
│  │  │     "postgres": { "url": "postgresql://..." },   │   │              │
│  │  │     "topology": { ... }                          │   │              │
│  │  │   }                                              │   │              │
│  │  └── HPA: CPU 60%, Memory 80%, 2-10 replicas       │   │              │
│  └─────────┬──────────────────────┬────────────────────┘   │              │
│            │                      │                        │              │
│  ┌─────────▼────────┐  ┌─────────▼──────────┐             │              │
│  │  PostgreSQL       │  │  NATS (3-node      │             │              │
│  │  (StatefulSet)    │  │   StatefulSet)     │             │              │
│  │                   │  │                    │             │              │
│  │  Actor state,     │  │  Inter-engine      │             │              │
│  │  session data,    │  │  pub/sub,          │             │              │
│  │  events, secrets  │  │  actor routing     │             │              │
│  └───────────────────┘  └────────────────────┘             │              │
│                                                            │              │
│  ┌───────────────┐                                         │              │
│  │    Redis      │  Cache (repos list, stale-while-        │              │
│  │               │  revalidate) — optional                 │              │
│  └───────────────┘                                         │              │
│                                                            │              │
│  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─┼─ ─ ─ ─ ─ ─  │
│  agent-sandbox operator (manages pods below)               │              │
│                                                            │              │
│  ┌──────────────────────────────────────────────┐          │              │
│  │ SandboxWarmPool                              │◄─────────┘              │
│  │  replicas: 3                                 │  SandboxClaim           │
│  │  templateRef: coding-agent-template          │                         │
│  └──────────┬───────────────────────────────────┘                         │
│             │ pre-warmed pods                                             │
│             ▼                                                             │
│  ┌──────────────────────────────────────────────┐                         │
│  │ Sandbox Pod (gVisor isolated)                │                         │
│  │                                              │                         │
│  │  ┌────────────────────────────────────────┐  │                         │
│  │  │ rivet sandbox-agent (Rust binary)      │  │                         │
│  │  │  POST /api/sessions/{id}/messages      │──┼── HTTP/SSE to app       │
│  │  │  GET  /api/sessions/{id}/events (SSE)  │  │                         │
│  │  └──────────────┬─────────────────────────┘  │                         │
│  │                 │ manages                     │                         │
│  │  ┌──────────────▼─────────────────────────┐  │                         │
│  │  │ Coding Agent (swappable)               │  │                         │
│  │  │  • Claude Code                         │  │                         │
│  │  │  • Codex                               │  │                         │
│  │  │  • OpenCode                            │  │                         │
│  │  │  • Amp                                 │  │                         │
│  │  └────────────────────────────────────────┘  │                         │
│  │                                              │                         │
│  │  git (repo clone, push via app token)        │                         │
│  └──────────────────────────────────────────────┘                         │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Infrastructure Components

### Rivet Engine (from [rivet-dev/rivet/k8s/engine](https://github.com/rivet-dev/rivet/tree/main/k8s/engine))

Rivet provides reference K8s manifests. The engine deployment has two components:

**Main Engine** — scalable workers (2-10 replicas via HPA):
```yaml
# 03-rivet-engine-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rivet-engine
  namespace: rivet-engine
spec:
  replicas: 2
  template:
    spec:
      containers:
      - name: rivet-engine
        image: rivetkit/engine:latest
        args: ["start", "--except-services", "singleton"]
        ports:
        - containerPort: 6420    # guard (actor routing)
          name: guard
        - containerPort: 6421    # api-peer (cluster internal)
          name: api-peer
        resources:
          requests: { cpu: "2000m", memory: "4Gi" }
          limits:   { cpu: "4000m", memory: "8Gi" }
        volumeMounts:
        - name: config
          mountPath: /etc/rivet
      volumes:
      - name: config
        configMap:
          name: engine-config
```

**Singleton Engine** — single-instance services (schedulers, coordinators):
```yaml
# 06-rivet-engine-singleton-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rivet-engine-singleton
spec:
  replicas: 1    # Always exactly 1
  template:
    spec:
      containers:
      - name: rivet-engine
        image: rivetkit/engine:latest
        args: ["start", "--only-services", "singleton"]
```

**Engine config** (`config.jsonc`):
```jsonc
{
  "postgres": {
    "url": "postgresql://postgres:postgres@postgres:5432/rivet"
  },
  "topology": {
    "datacenter_label": 1,
    "datacenters": [{
      "name": "production",
      "datacenter_label": 1,
      "is_leader": true,
      "public_url": "https://engine.open-inspect.example.com:6420",
      "peer_url": "http://rivet-engine.open-inspect.svc.cluster.local:6421",
      "proxy_url": "http://rivet-engine.open-inspect.svc.cluster.local:6420"
    }]
  }
}
```

### NATS (3-node StatefulSet)

Required by Rivet Engine for inter-process actor routing and pub/sub:

```yaml
# 3-node NATS cluster via StatefulSet
# Routes autodiscovered via K8s DNS:
#   nats-0.nats.rivet-engine.svc.cluster.local:6222
#   nats-1.nats.rivet-engine.svc.cluster.local:6222
#   nats-2.nats.rivet-engine.svc.cluster.local:6222
```

### PostgreSQL (StatefulSet)

Stores actor state, session data, events, secrets. For production, use CloudNativePG, RDS, or
Cloud SQL instead of the reference StatefulSet.

### agent-sandbox CRDs

```yaml
apiVersion: extensions.agents.x-k8s.io/v1alpha1
kind: SandboxTemplate
metadata:
  name: coding-agent
spec:
  podTemplate:
    spec:
      runtimeClassName: gvisor
      containers:
      - name: agent
        image: open-inspect/sandbox:latest
        ports:
        - containerPort: 2468
        readinessProbe:
          httpGet:
            path: /health
            port: 2468
          periodSeconds: 1
        resources:
          requests:
            cpu: "500m"
            memory: "1Gi"
            ephemeral-storage: "2Gi"
      restartPolicy: OnFailure
---
apiVersion: extensions.agents.x-k8s.io/v1alpha1
kind: SandboxWarmPool
metadata:
  name: coding-agent-pool
spec:
  replicas: 3
  sandboxTemplateRef:
    name: coding-agent
```

---

## How RivetKit Actors Connect to Engine

Your app connects to Rivet Engine via environment variables. The Engine handles actor routing,
state persistence, and lifecycle — your actor code just extends `Actor`:

```typescript
import { Actor } from "@rivetkit/actor";

// Environment variables set by K8s Secret:
//   RIVET_ENDPOINT=http://rivet-engine.open-inspect.svc.cluster.local:6420
//   RIVET_PUBLIC_ENDPOINT=https://engine.open-inspect.example.com:6420

interface SessionState {
  id: string;
  repoOwner: string;
  repoName: string;
  status: string;
  sandboxIp: string | null;
  events: SandboxEvent[];
}

class SessionActor extends Actor<SessionState> {
  // Engine persists this to PostgreSQL automatically.
  // On hibernation + wake, state is restored from PG.
  // On scale event, actors migrate between replicas.
  override initialize(): SessionState {
    return {
      id: "",
      repoOwner: "",
      repoName: "",
      status: "created",
      sandboxIp: null,
      events: [],
    };
  }

  // RPC — called from Hono HTTP router via Engine's guard port.
  // Engine routes the request to whichever replica holds this actor.
  async createSession(config: SessionConfig): Promise<void> {
    this.state.id = config.sessionId;
    this.state.repoOwner = config.repoOwner;
    this.state.repoName = config.repoName;

    // Create sandbox via K8s API
    const claimName = await this.k8sProvider.createSandboxClaim(config);
    this.state.status = "spawning";
  }

  async enqueuePrompt(content: string, author: Author): Promise<string> {
    const messageId = generateId();
    // State mutations are automatically persisted
    this.state.messageQueue.push({ id: messageId, content, author, status: "pending" });
    await this.dispatchNextMessage();
    return messageId;
  }

  // WebSocket — Engine handles connection routing to the correct replica.
  // If this actor migrates to a different replica, WS connections follow.
  onConnect(conn: Connection): void {
    conn.send({ type: "subscribed", state: this.getPublicState() });
    for (const event of this.state.events) {
      conn.send({ type: "sandbox_event", event });
    }
  }

  // SSE bridge from sandbox-agent
  private async startEventBridge(): Promise<void> {
    const es = new EventSource(
      `http://${this.state.sandboxIp}:2468/api/sessions/${this.state.id}/events`
    );
    es.onmessage = (e) => {
      const event = JSON.parse(e.data);
      this.state.events.push(event);      // Persisted by Engine
      this.broadcast({ type: "sandbox_event", event });  // To all WS clients
    };
  }
}
```

**What Engine gives you that a plain K8s Deployment cannot:**
- `this.state` mutations are automatically persisted to PostgreSQL
- WebSocket connections route to the replica holding the actor, not random round-robin
- Idle actors hibernate (freed from memory) and wake instantly on next request
- NATS handles cross-replica actor discovery when scaling

---

## What's Different from the Current Architecture

| Concern | Current (CF + Modal) | New (K8s + Rivet Engine) |
|---------|---------------------|--------------------------|
| **Actor orchestration** | Cloudflare Durable Objects runtime | Rivet Engine (Rust binary, HPA 2-10) |
| **Actor routing** | CF edge → DO stub routing | Engine guard port → NATS → correct replica |
| **State persistence** | DO SQLite (per-actor) | Engine → PostgreSQL (automatic) |
| **Actor hibernation** | CF hibernation + wake | Engine hibernation + instant wake |
| **WebSocket routing** | CF WebSocket hibernation tags | Engine connection routing |
| **Inter-actor comms** | N/A | NATS pub/sub |
| **Session state** | DO SQLite tables | RivetKit `this.state` (auto-persisted) |
| **Sandbox lifecycle** | Modal HTTP API | `agent-sandbox` CRDs + operator |
| **Warm starts** | Modal snapshot restore | `SandboxWarmPool` (<1s allocation) |
| **Agent comms** | Custom `bridge.py` + OpenCode SSE | Sandbox Agent SDK (HTTP/SSE) |
| **Agent support** | OpenCode only | Claude Code, Codex, OpenCode, Amp |
| **Isolation** | Modal containers | gVisor or Kata Containers |
| **Cache** | Cloudflare KV | Redis (optional) |
| **Database** | D1 + DO SQLite | PostgreSQL (shared, Engine-managed) |
| **Message bus** | N/A | NATS (3-node cluster) |
| **Scaling** | CF auto-scaling | K8s HPA (CPU 60%, Mem 80%) |

---

## Full Component Inventory

| Component | What | How |
|-----------|------|-----|
| **Rivet Engine** | Actor orchestration, routing, persistence | Deployment (2-10 replicas) + Singleton (1 replica) |
| **NATS** | Inter-engine messaging, actor discovery | StatefulSet (3 replicas) |
| **PostgreSQL** | Actor state, session data, events, secrets | StatefulSet or managed (RDS/CloudSQL/CloudNativePG) |
| **Redis** | Repos cache (stale-while-revalidate) | Deployment (1 replica) or managed |
| **App** | HTTP router, RivetKit actors, sandbox provider | Deployment (2+ replicas) |
| **agent-sandbox operator** | Sandbox pod lifecycle | Operator + CRDs (installed once) |
| **SandboxWarmPool** | Pre-warmed agent pods | CR (3+ warm pods) |
| **Sandbox pods** | Isolated agent execution | Claimed from warm pool per session |
| **Ingress** | TLS, WS upgrade, routing | nginx/traefik with WS annotations |

---

## Implementation Order

```
Phase 1: Engine Infrastructure
  ├── Deploy NATS StatefulSet (3 nodes)
  ├── Deploy PostgreSQL (StatefulSet or managed)
  ├── Deploy Rivet Engine (main + singleton)
  ├── Verify: curl http://localhost:6421/health
  └── Deploy Redis for caching

Phase 2: App + Actors
  ├── RivetKit SessionActor (state, WS, message queue)
  ├── Hono HTTP router (sessions, repos, secrets)
  ├── Connect app to Engine via RIVET_ENDPOINT
  └── Verify: create session, send prompt, get state

Phase 3: Sandbox Infrastructure
  ├── Install agent-sandbox operator + CRDs
  ├── Define SandboxTemplate (container with sandbox-agent + git)
  ├── Define SandboxWarmPool (3 pre-warmed pods)
  ├── K8s SandboxProvider in app (creates SandboxClaims)
  └── Verify: claim pod, check readiness <1s

Phase 4: Agent Integration
  ├── HTTP/SSE client for sandbox-agent API
  ├── Event bridge (SSE → actor state → WS broadcast)
  ├── Prompt dispatch (message queue → sandbox-agent POST)
  └── Permission handling (human-in-the-loop)

Phase 5: Git + PR Flow
  ├── Git clone/push inside sandbox
  ├── PR creation via GitHub API
  └── Source control provider abstraction

Phase 6: Production Hardening
  ├── Helm chart packaging all components
  ├── external-secrets-operator
  ├── Ingress with TLS + WebSocket support
  ├── Prometheus metrics + Loki logs
  └── Network policies for sandbox isolation
```

---

## References

- [Rivet K8s manifests](https://github.com/rivet-dev/rivet/tree/main/k8s/engine) — reference
  deployments for Engine, NATS, PostgreSQL
- [Rivet Engine](https://github.com/rivet-dev/engine) — the orchestration binary
- [RivetKit](https://github.com/rivet-dev/rivetkit) — actor framework library
- [Rivet self-hosting docs](https://www.rivet.dev/docs/self-hosting/kubernetes/) — K8s deployment
  guide with PostgreSQL
- [kubernetes-sigs/agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox) — sandbox CRDs
- [Rivet Sandbox Agent SDK](https://github.com/rivet-dev/sandbox-agent) — universal agent API
- [Agent Sandbox docs](https://agent-sandbox.sigs.k8s.io/) — CRD reference
