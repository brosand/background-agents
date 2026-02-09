# Control Plane Service
#
# Replaces Cloudflare Workers deployment.
# Runs as a Node.js HTTP server with Hono + WebSocket + PostgreSQL + Redis.

FROM node:22-alpine AS builder

WORKDIR /app

# Copy package files
COPY package.json tsconfig.json ./
COPY packages/shared/package.json packages/shared/
COPY packages/control-plane/package.json packages/control-plane/

# Install dependencies
RUN npm install

# Copy source
COPY packages/shared/ packages/shared/
COPY packages/control-plane/ packages/control-plane/

# Build
RUN npm run build -w packages/shared && npm run build -w packages/control-plane

FROM node:22-alpine

WORKDIR /app

COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/packages/shared/dist ./packages/shared/dist
COPY --from=builder /app/packages/shared/package.json ./packages/shared/
COPY --from=builder /app/packages/control-plane/dist ./packages/control-plane/dist
COPY --from=builder /app/packages/control-plane/package.json ./packages/control-plane/
# Copy SQL schema file
COPY --from=builder /app/packages/control-plane/src/db/schema.sql ./packages/control-plane/dist/db/

EXPOSE 8787

CMD ["node", "packages/control-plane/dist/server.js"]
