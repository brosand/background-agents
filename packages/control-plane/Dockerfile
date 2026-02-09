FROM node:22-slim AS builder

WORKDIR /app

# Copy package files
COPY package.json ./
COPY packages/shared/package.json packages/shared/
COPY packages/control-plane/package.json packages/control-plane/

# Install dependencies
RUN npm install --workspace=packages/control-plane

# Copy source
COPY packages/shared/ packages/shared/
COPY packages/control-plane/ packages/control-plane/

# Build
RUN npm run build --workspace=packages/control-plane

# Runtime
FROM node:22-slim

WORKDIR /app

COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/packages/shared ./packages/shared
COPY --from=builder /app/packages/control-plane/dist ./packages/control-plane/dist
COPY --from=builder /app/packages/control-plane/package.json ./packages/control-plane/
COPY --from=builder /app/packages/control-plane/src/db/schema.sql ./packages/control-plane/src/db/

ENV NODE_ENV=production

EXPOSE 3001

CMD ["node", "packages/control-plane/dist/index.js"]
