# Multi-stage build for the K8s control plane
FROM node:22-alpine AS builder

WORKDIR /app

# Copy workspace files for dependency resolution
COPY package.json tsconfig.json ./
COPY packages/shared/package.json packages/shared/
COPY packages/control-plane-k8s/package.json packages/control-plane-k8s/

# Install dependencies
RUN npm install --workspace=packages/control-plane-k8s

# Copy source
COPY packages/shared/ packages/shared/
COPY packages/control-plane-k8s/ packages/control-plane-k8s/

# Build
RUN npm run build --workspace=packages/control-plane-k8s

# --- Production stage ---
FROM node:22-alpine

RUN apk add --no-cache tini
WORKDIR /app

# Copy built output and dependencies
COPY --from=builder /app/packages/control-plane-k8s/dist ./dist
COPY --from=builder /app/packages/control-plane-k8s/package.json ./
COPY --from=builder /app/packages/control-plane-k8s/src/db/schema.sql ./dist/db/schema.sql
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/packages/shared ./node_modules/@open-inspect/shared

# Non-root user
RUN addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://localhost:8080/health || exit 1

EXPOSE 8080

ENTRYPOINT ["tini", "--"]
CMD ["node", "dist/index.js"]
