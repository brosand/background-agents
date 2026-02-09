# Kubernetes-Native Architecture: Clean-Slate Design

How to rebuild Open-Inspect on Kubernetes using `agent-sandbox` CRDs, Rivet Sandbox Agent SDK,
and RivetKit — without Cloudflare or Modal.

## The Three Tools and What They Replace

| Tool | Replaces | Layer |
|------|----------|-------|
| **[`kubernetes-sigs/agent-sandbox`](https://github.com/kubernetes-sigs/agent-sandbox)** | Modal (container lifecycle, warm pools, snapshots) | Infrastructure |
| **[Rivet Sandbox Agent SDK](https://github.com/rivet-dev/sandbox-agent)** | `bridge.py` + OpenCode-specific integration | Agent abstraction |
| **[RivetKit](https://github.com/rivet-dev/rivetkit)** | Cloudflare Durable Objects (session state, WebSockets) | Orchestration |

Rivet Engine is **not needed**. RivetKit runs standalone as a TypeScript library with a Redis
driver. Kubernetes handles everything Engine would do (scheduling, scaling, health checks).

---

## Architecture

```
                           ┌──────────────────────────────┐
                           │       Web App / Slack         │
                           └─────────────┬────────────────┘
                                         │ HTTP + WebSocket
                                         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  K8s Deployment: Control Plane                                          │
│                                                                         │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │  Node.js process                                                   │  │
│  │                                                                    │  │
│  │  ┌──────────────┐  ┌────────────────────────┐  ┌───────────────┐  │  │
│  │  │ Hono HTTP    │  │ RivetKit SessionActor  │  │ K8s Sandbox   │  │  │
│  │  │ router       │──│ (per-session state,    │──│ Provider      │  │  │
│  │  │              │  │  WS coordination,      │  │ (creates CRs) │  │  │
│  │  │ /sessions    │  │  message queue,        │  │               │  │  │
│  │  │ /repos       │  │  event log)            │  │ kubectl API   │  │  │
│  │  │ /health      │  │                        │  │               │  │  │
│  │  └──────────────┘  └────────────────────────┘  └───────┬───────┘  │  │
│  │                                                        │          │  │
│  └────────────────────────────────────────────────────────┼──────────┘  │
│                                                           │             │
│  ┌────────────────┐  ┌──────────────┐                     │             │
│  │  PostgreSQL    │  │    Redis     │                     │             │
│  │  (sessions,    │  │  (RivetKit   │                     │             │
│  │   events,      │  │   driver,    │                     │             │
│  │   secrets)     │  │   cache)     │                     │             │
│  └────────────────┘  └──────────────┘                     │             │
│                                                           │             │
│  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┼ ─ ─ ─ ─ ─  │
│  agent-sandbox operator (manages pods below)              │             │
│                                                           │             │
│  ┌─────────────────────────────────────────────┐          │             │
│  │ SandboxWarmPool                             │◄─────────┘             │
│  │  replicas: 3                                │  SandboxClaim          │
│  │  templateRef: coding-agent-template         │                        │
│  └──────────┬──────────────────────────────────┘                        │
│             │ pre-warmed pods                                           │
│             ▼                                                           │
│  ┌─────────────────────────────────────────────┐                        │
│  │ Sandbox Pod (gVisor isolated)               │                        │
│  │                                             │                        │
│  │  ┌───────────────────────────────────────┐  │                        │
│  │  │ rivet sandbox-agent (Rust binary)     │  │                        │
│  │  │  POST /api/sessions/{id}/messages     │──┼── HTTP/SSE to control  │
│  │  │  GET  /api/sessions/{id}/events (SSE) │  │   plane SessionActor   │
│  │  │  POST /api/sessions/{id}/permissions  │  │                        │
│  │  └──────────────┬────────────────────────┘  │                        │
│  │                 │ manages                    │                        │
│  │  ┌──────────────▼────────────────────────┐  │                        │
│  │  │ Coding Agent (swappable)              │  │                        │
│  │  │  • Claude Code                        │  │                        │
│  │  │  • Codex                              │  │                        │
│  │  │  • OpenCode                           │  │                        │
│  │  │  • Amp                                │  │                        │
│  │  └───────────────────────────────────────┘  │                        │
│  │                                             │                        │
│  │  ┌───────────────────────────────────────┐  │                        │
│  │  │ git (repo clone, push via app token)  │  │                        │
│  │  └───────────────────────────────────────┘  │                        │
│  └─────────────────────────────────────────────┘                        │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## How the Pieces Fit Together

### 1. `agent-sandbox` CRDs — the infrastructure layer

The `agent-sandbox` operator manages sandbox pod lifecycle. You define:

```yaml
# What a sandbox looks like
apiVersion: extensions.agents.x-k8s.io/v1alpha1
kind: SandboxTemplate
metadata:
  name: coding-agent
spec:
  podTemplate:
    spec:
      runtimeClassName: gvisor           # or kata for VM isolation
      containers:
      - name: agent
        image: open-inspect/sandbox:latest  # contains sandbox-agent + git
        ports:
        - containerPort: 2468            # sandbox-agent HTTP/SSE
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
        env:
        - name: SANDBOX_AGENT_TOKEN
          valueFrom:
            secretKeyRef:
              name: sandbox-tokens
              key: agent-token
      restartPolicy: OnFailure
---
# Keep 3 pods warm and ready
apiVersion: extensions.agents.x-k8s.io/v1alpha1
kind: SandboxWarmPool
metadata:
  name: coding-agent-pool
spec:
  replicas: 3
  sandboxTemplateRef:
    name: coding-agent
```

When a session needs a sandbox, the control plane creates a `SandboxClaim`:

```yaml
apiVersion: extensions.agents.x-k8s.io/v1alpha1
kind: SandboxClaim
metadata:
  name: session-abc123
  labels:
    open-inspect/session-id: abc123
spec:
  sandboxTemplateRef:
    name: coding-agent
```

The warm pool allocates a pre-warmed pod in **<1 second** (vs Modal's cold-start).

**What this replaces:**
- `ModalSandboxProvider.createSandbox()` → create `SandboxClaim` CR
- Modal filesystem snapshots → K8s PV snapshots (or GKE Pod Snapshots)
- Modal's warm start via snapshot restore → `SandboxWarmPool` pre-warming
- Container isolation → gVisor / Kata Containers

### 2. Rivet Sandbox Agent SDK — the agent abstraction layer

Runs **inside** each sandbox pod as a ~15MB static Rust binary. Replaces `bridge.py` and the
OpenCode-specific SSE parsing.

```dockerfile
# Sandbox container image
FROM ubuntu:22.04

# Install sandbox-agent (universal agent API)
RUN curl -fsSL https://releases.rivet.dev/sandbox-agent/latest/install.sh | sh

# Install git for repo operations
RUN apt-get update && apt-get install -y git

# Pre-install agents you want to support
RUN sandbox-agent install-agent claude-code
RUN sandbox-agent install-agent codex
RUN sandbox-agent install-agent opencode

EXPOSE 2468
CMD ["sandbox-agent", "server", "--host", "0.0.0.0", "--port", "2468"]
```

The control plane talks to it over HTTP/SSE:

```typescript
// Create a session with whatever agent you want
await fetch(`http://${sandboxIp}:2468/api/sessions/${sessionId}`, {
  method: "POST",
  body: JSON.stringify({
    agent: "claude-code",        // or "codex", "opencode", "amp"
    permissionMode: "auto",      // or "manual" for human-in-the-loop
  }),
});

// Send a prompt
await fetch(`http://${sandboxIp}:2468/api/sessions/${sessionId}/messages`, {
  method: "POST",
  body: JSON.stringify({ message: "Fix the failing tests" }),
});

// Stream events (universal schema regardless of agent)
const events = new EventSource(
  `http://${sandboxIp}:2468/api/sessions/${sessionId}/events`
);
events.onmessage = (e) => {
  const event = JSON.parse(e.data);
  // event.type: "tool_call" | "file_edit" | "permission_request" | ...
  // Same schema whether running Claude Code, Codex, or OpenCode
};
```

**What this replaces:**
- `bridge.py` (500+ lines of SSE parsing, heartbeats, event normalization)
- OpenCode-specific HTTP client code
- Custom event schema → universal event types
- Single-agent lock-in → swap agents via config

### 3. RivetKit — the orchestration layer

Runs inside the control plane Node.js process. Each session gets a RivetKit Actor that holds
state in memory, persists to PostgreSQL via Redis driver, and coordinates WebSocket clients
with sandbox events.

```typescript
import { Actor, type ActorContext } from "@rivetkit/actor";

interface SessionState {
  id: string;
  repoOwner: string;
  repoName: string;
  status: string;
  sandboxClaimName: string | null;
  sandboxIp: string | null;
  messageQueue: Message[];
  events: SandboxEvent[];
}

class SessionActor extends Actor<SessionState> {
  // State is automatically persisted by RivetKit
  override initialize(): SessionState {
    return {
      id: "",
      repoOwner: "",
      repoName: "",
      status: "created",
      sandboxClaimName: null,
      sandboxIp: null,
      messageQueue: [],
      events: [],
    };
  }

  // RPC methods — called from the HTTP router

  async createSession(config: SessionConfig): Promise<void> {
    this.state.id = config.sessionId;
    this.state.repoOwner = config.repoOwner;
    this.state.repoName = config.repoName;

    // Create a SandboxClaim via K8s API
    const claimName = await this.k8sProvider.createSandboxClaim(config);
    this.state.sandboxClaimName = claimName;
    this.state.status = "spawning";
  }

  async enqueuePrompt(content: string, author: Author): Promise<string> {
    const messageId = generateId();
    this.state.messageQueue.push({ id: messageId, content, author, status: "pending" });

    // Forward to sandbox-agent via HTTP
    await this.dispatchNextMessage();
    return messageId;
  }

  // WebSocket connections — RivetKit manages these natively

  onConnect(conn: Connection): void {
    // Send current state + event replay
    conn.send({ type: "subscribed", state: this.getPublicState() });
    for (const event of this.state.events) {
      conn.send({ type: "sandbox_event", event });
    }
  }

  // SSE event bridge — streams events from sandbox-agent to all clients

  private async startEventBridge(): Promise<void> {
    const es = new EventSource(
      `http://${this.state.sandboxIp}:2468/api/sessions/${this.state.id}/events`
    );

    es.onmessage = (e) => {
      const event = JSON.parse(e.data);
      this.state.events.push(event);  // Persisted by RivetKit

      // Broadcast to all connected WebSocket clients
      this.broadcast({ type: "sandbox_event", event });

      if (event.type === "session_ended") {
        this.handleExecutionComplete(event);
      }
    };
  }
}
```

**What this replaces:**
- Cloudflare Durable Objects (`SessionDO`)
- DO SQLite (state persists via RivetKit's storage driver)
- `ctx.acceptWebSocket()` + hibernation tags → RivetKit connection management
- `ctx.storage.setAlarm()` → `setTimeout` (actors are long-lived)
- `ctx.waitUntil()` → plain async (no request lifecycle limits)

---

## Communication Flow

```
User types a prompt
        │
        ▼
   ┌─────────┐     HTTP POST /sessions/:id/prompt
   │ Web App  │────────────────────────────────────────┐
   │          │     WebSocket (real-time events)       │
   │          │◄───────────────────────────────────┐   │
   └─────────┘                                     │   │
                                                   │   │
   ┌───────────────────────────────────────────┐   │   │
   │ Control Plane (RivetKit SessionActor)     │   │   │
   │                                           │   │   │
   │  1. Enqueue message in state              │◄──┘───┘
   │  2. If no sandbox, create SandboxClaim    │
   │  3. Wait for pod ready (warm pool: <1s)   │
   │  4. POST prompt to sandbox-agent          │───────┐
   │  5. Stream SSE events from sandbox-agent  │◄──┐   │
   │  6. Persist events to state               │   │   │
   │  7. Broadcast to WebSocket clients        │───┘   │
   │  8. On completion, process next in queue   │       │
   └───────────────────────────────────────────┘       │
                                                       │
   ┌───────────────────────────────────────────┐       │
   │ Sandbox Pod (agent-sandbox CRD)           │       │
   │                                           │       │
   │  sandbox-agent (Rust binary)              │◄──────┘
   │    ├── receives prompt via HTTP            │
   │    ├── runs Claude Code / Codex / OpenCode │
   │    ├── streams events via SSE              │
   │    └── handles permissions (auto/manual)   │
   │                                           │
   │  git clone → work → git push              │
   └───────────────────────────────────────────┘
```

---

## What's Different from the Current Architecture

| Concern | Current (CF + Modal) | New (K8s native) |
|---------|---------------------|-----------------|
| **Session state** | Durable Object + SQLite | RivetKit Actor + PostgreSQL |
| **WebSockets** | CF WebSocket hibernation | RivetKit connections (long-lived) |
| **Sandbox lifecycle** | Modal HTTP API | `agent-sandbox` CRDs + operator |
| **Warm starts** | Modal snapshot restore | `SandboxWarmPool` pre-warmed pods |
| **Agent communication** | Custom `bridge.py` + OpenCode SSE | Sandbox Agent SDK (HTTP/SSE) |
| **Agent support** | OpenCode only | Claude Code, Codex, OpenCode, Amp |
| **Isolation** | Modal containers | gVisor or Kata Containers |
| **Snapshots** | Modal filesystem snapshots | K8s PV snapshots / GKE Pod Snapshots |
| **Cache** | Cloudflare KV | Redis |
| **Database** | D1 + DO SQLite | PostgreSQL |
| **Scaling** | CF auto-scaling | K8s HPA/KEDA |
| **Alarms/timers** | DO Alarms | `setTimeout` (actors are long-lived) |
| **Background work** | `ctx.waitUntil()` | Plain async calls |
| **Secrets** | CF Worker secrets + Terraform | K8s Secrets + external-secrets-operator |
| **Deployment** | Wrangler + Terraform CF provider | Helm / Kustomize |
| **Observability** | CF Workers Logs | Container logs → Loki/Datadog |

---

## Key Advantages

1. **Agent-swappable** — change `agent: "claude-code"` to `agent: "codex"` in one line. No code
   changes. The Sandbox Agent SDK normalizes everything.

2. **Sub-second sandbox allocation** — `SandboxWarmPool` keeps pods running and ready. No cold
   starts, no snapshot restore dance. Just claim a pod.

3. **No vendor lock-in** — every component is open source and runs on any K8s cluster (EKS, GKE,
   AKS, k3s, bare metal). No Cloudflare, no Modal.

4. **Simpler bridge** — `bridge.py` is 500+ lines of custom SSE parsing, heartbeat management,
   and event normalization. Sandbox Agent SDK is a single `curl | sh` install that handles all of
   this.

5. **Standard K8s patterns** — CRDs, operators, HPA, PVs, Secrets, Ingress. Your ops team already
   knows these tools.

6. **Better isolation** — gVisor (process-level) or Kata (VM-level) isolation via `runtimeClassName`.
   Modal provides container isolation but you can't choose the isolation backend.

---

## What Rivet Engine Is (and Why You Skip It)

Rivet Engine is a Rust binary that orchestrates RivetKit actors across infrastructure — scheduling
them onto nodes, handling hibernation, managing multi-region distribution.

**You don't need it because:**
- K8s Deployments already schedule your control plane pods
- K8s HPA/KEDA already handles scaling
- RivetKit's Redis driver handles actor state persistence
- `agent-sandbox` operator handles sandbox pod lifecycle
- Your actors are long-lived (no hibernation needed)

Engine adds value for: auto-scaling actors to zero, multi-region edge deployment, or running
thousands of actors across a fleet. If you need those later, you can add Engine without changing
your actor code (RivetKit is the same API either way).

---

## Implementation Order

```
Phase 1: Foundation
  ├── PostgreSQL schema + stores
  ├── Redis setup
  ├── Hono HTTP server with health + session CRUD
  └── RivetKit SessionActor (state, WS, message queue)

Phase 2: Sandbox Infrastructure
  ├── Install agent-sandbox operator
  ├── Define SandboxTemplate + SandboxWarmPool
  ├── Build sandbox container image (sandbox-agent + git + agents)
  ├── K8s SandboxProvider (creates SandboxClaims via K8s API)
  └── Sandbox readiness detection (watch pod IP)

Phase 3: Agent Integration
  ├── HTTP/SSE client for sandbox-agent API
  ├── Event bridge (SSE → RivetKit state → WebSocket broadcast)
  ├── Prompt dispatch (message queue → sandbox-agent POST)
  └── Permission handling (human-in-the-loop flow)

Phase 4: Git + PR Flow
  ├── Git clone/push inside sandbox (via sandbox-agent or sidecar)
  ├── PR creation via GitHub API (from control plane)
  └── Source control provider abstraction

Phase 5: Production
  ├── Helm chart with all components
  ├── external-secrets-operator for secret management
  ├── Ingress with TLS + WebSocket support
  ├── Monitoring (Prometheus metrics, Loki logs)
  └── HPA for control plane scaling
```

---

## References

- [kubernetes-sigs/agent-sandbox (GitHub)](https://github.com/kubernetes-sigs/agent-sandbox)
- [Agent Sandbox docs](https://agent-sandbox.sigs.k8s.io/)
- [Rivet Sandbox Agent SDK](https://github.com/rivet-dev/sandbox-agent)
- [Sandbox Agent docs](https://sandboxagent.dev/)
- [RivetKit (GitHub)](https://github.com/rivet-dev/rivetkit)
- [Introducing Sandbox Agent SDK](https://www.rivet.dev/changelog/2026-01-28-sandbox-agent-sdk/)
- [Google: Why K8s needs agent execution standards](https://opensource.googleblog.com/2025/11/unleashing-autonomous-ai-agents-why-kubernetes-needs-a-new-standard-for-agent-execution.html)
- [GKE Agent Sandbox How-To](https://docs.google.com/kubernetes-engine/docs/how-to/agent-sandbox)
