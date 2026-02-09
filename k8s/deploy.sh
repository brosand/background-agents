#!/usr/bin/env bash
# Deploy Open-Inspect to Kubernetes.
#
# Prerequisites:
# - kubectl configured with target cluster
# - Container images built and pushed to registry
# - secrets.yaml populated with actual values
#
# Usage:
#   ./deploy.sh                    # Apply all manifests
#   ./deploy.sh --dry-run=client   # Preview changes

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Deploying Open-Inspect to Kubernetes"

# Apply using kustomize
kubectl apply -k "$SCRIPT_DIR" "$@"

echo ""
echo "==> Waiting for deployments..."
kubectl -n open-inspect rollout status deployment/postgres --timeout=120s
kubectl -n open-inspect rollout status deployment/redis --timeout=60s
kubectl -n open-inspect rollout status deployment/control-plane --timeout=120s
kubectl -n open-inspect rollout status deployment/sandbox-api --timeout=120s
kubectl -n open-inspect rollout status deployment/web --timeout=120s

echo ""
echo "==> Deployment complete!"
echo ""
echo "Services:"
kubectl -n open-inspect get svc
echo ""
echo "Pods:"
kubectl -n open-inspect get pods
