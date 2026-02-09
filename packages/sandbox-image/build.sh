#!/usr/bin/env bash
# Build the sandbox container image.
#
# Usage:
#   ./build.sh                          # Build with default tag
#   ./build.sh my-registry/sandbox:v1   # Build with custom tag
#
# The sandbox code is copied from packages/modal-infra/src/sandbox/ since those
# files (entrypoint.py, bridge.py, types.py) are infrastructure-agnostic.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
IMAGE_TAG="${1:-open-inspect-sandbox:latest}"

# Copy sandbox code next to the Dockerfile so COPY works
SANDBOX_SRC="$REPO_ROOT/packages/modal-infra/src/sandbox"
SANDBOX_DST="$SCRIPT_DIR/sandbox"

echo "Copying sandbox code from $SANDBOX_SRC to $SANDBOX_DST..."
rm -rf "$SANDBOX_DST"
cp -r "$SANDBOX_SRC" "$SANDBOX_DST"

# Also copy the log_config module since sandbox code imports it
if [ -f "$REPO_ROOT/packages/modal-infra/src/log_config.py" ]; then
  cp "$REPO_ROOT/packages/modal-infra/src/log_config.py" "$SANDBOX_DST/"
fi

echo "Building image: $IMAGE_TAG"
docker build -t "$IMAGE_TAG" "$SCRIPT_DIR"

# Clean up copied sandbox code
rm -rf "$SANDBOX_DST"

echo "Built: $IMAGE_TAG"
