FROM python:3.12-slim-bookworm

# System packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl git openssh-client build-essential jq \
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

# Python dependencies
COPY pyproject.toml /app/
RUN pip install --no-cache-dir -e /app

# Install Playwright (for browser automation)
RUN pip install --no-cache-dir playwright \
    && playwright install --with-deps chromium

# Install OpenCode CLI
RUN npm install -g opencode@latest

# Copy sandbox source
COPY src/ /app/sandbox/

ENV PYTHONPATH=/app
WORKDIR /workspace

ENTRYPOINT ["python", "-m", "sandbox.entrypoint"]
