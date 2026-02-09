# Open-Inspect Sandbox Image
#
# Complete development environment for coding agent sandboxes.
# Replaces the Modal image builder (packages/modal-infra/src/images/base.py).
#
# Build: docker build -t open-inspect-sandbox:latest packages/sandbox-image/
# Push:  docker push <registry>/open-inspect-sandbox:latest

FROM python:3.12-slim AS base

# Cache buster - change to force rebuild
ARG CACHE_BUSTER=v1-k8s-migration

# System packages (including Playwright dependencies)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    build-essential \
    ca-certificates \
    gnupg \
    openssh-client \
    jq \
    unzip \
    # Playwright browser dependencies
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libcairo2 \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js 22 LTS
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs \
    && node --version \
    && npm --version

# Install pnpm and Bun
RUN npm install -g pnpm@latest \
    && pnpm --version \
    && curl -fsSL https://bun.sh/install | bash \
    && echo 'export BUN_INSTALL="$HOME/.bun"' >> /etc/profile.d/bun.sh \
    && echo 'export PATH="$BUN_INSTALL/bin:$PATH"' >> /etc/profile.d/bun.sh

# Install Python tools
RUN pip install --no-cache-dir \
    uv \
    httpx \
    websockets \
    playwright \
    "pydantic>=2.0" \
    "PyJWT[crypto]"

# Install OpenCode CLI and plugin for custom tools
RUN npm install -g opencode-ai@latest \
    && npm install -g @opencode-ai/plugin@latest zod \
    && opencode --version || echo 'OpenCode installed'

# Install Playwright browsers (Chromium only to save space)
RUN playwright install chromium \
    && playwright install-deps chromium

# Create working directories
RUN mkdir -p /workspace /app/plugins /tmp/opencode

# Environment variables
ENV HOME=/root \
    NODE_ENV=development \
    PNPM_HOME=/root/.local/share/pnpm \
    PATH=/root/.bun/bin:/root/.local/share/pnpm:/usr/local/bin:/usr/bin:/bin \
    PLAYWRIGHT_BROWSERS_PATH=/root/.cache/ms-playwright \
    PYTHONPATH=/app \
    SANDBOX_VERSION=${CACHE_BUSTER} \
    NODE_PATH=/usr/lib/node_modules

# Copy sandbox code (entrypoint, bridge, types) into the image
# These files are portable - they have no Modal dependencies
COPY sandbox/ /app/sandbox/

WORKDIR /workspace

# The entrypoint supervisor manages OpenCode + bridge processes
CMD ["python", "-m", "sandbox.entrypoint"]
