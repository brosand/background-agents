FROM debian:bookworm-slim

# System packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl git openssh-client ca-certificates build-essential jq \
    && rm -rf /var/lib/apt/lists/*

# Node.js 22
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# pnpm
RUN npm install -g pnpm@9

# Bun
RUN curl -fsSL https://bun.sh/install | bash
ENV PATH="/root/.bun/bin:$PATH"

# Install sandbox-agent binary
RUN curl -fsSL https://releases.rivet.dev/sandbox-agent/latest/install.sh | sh
ENV PATH="/root/.sandbox-agent/bin:$PATH"

# Install Claude Code (primary agent)
RUN npm install -g @anthropic-ai/claude-code

# Install Playwright (for browser automation)
RUN npx playwright install --with-deps chromium

# Working directory for cloned repos
WORKDIR /workspace

# sandbox-agent listens on port 2468 by default
EXPOSE 2468

# Start sandbox-agent server
# SANDBOX_AGENT_TOKEN must be set as an environment variable
ENTRYPOINT ["sandbox-agent", "server", "--host", "0.0.0.0", "--port", "2468"]
