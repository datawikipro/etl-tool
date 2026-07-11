#!/usr/bin/env bash
# deploy-ci.sh
# ETL Tool CI/CD script for GitHub Actions

set -e

echo -e "\033[0;36m=== ETL-Tool CI/CD Build ===\033[0m"
IMAGE_NAME="ghcr.io/datawikipro/etl-tool:latest"
REMOTE_HOST="chernousov_a@100.89.122.84"

# 1. GHCR Authentication
if [ -z "$GITHUB_TOKEN" ]; then
    echo "ERROR: GITHUB_TOKEN is not set"
    exit 1
fi

echo -e "\033[0;36m[Phase 1] Authenticating to GHCR...\033[0m"
echo "$GITHUB_TOKEN" | podman login ghcr.io -u datawikipro --password-stdin

# 2. Build & Push Docker image
echo -e "\033[0;36m[Phase 2] Building and pushing image...\033[0m"
podman build -f Dockerfile -t "$IMAGE_NAME" .
podman push --creds "datawikipro:${GITHUB_TOKEN}" "$IMAGE_NAME"

# 3. Import image to K3s on target server (Removed - node 100.89.122.84 is deprecated)
# echo -e "\033[0;36m[Phase 3] Pulling new image into K3S on remote server...\033[0m"
# ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=15 $REMOTE_HOST "sudo /usr/local/bin/k3s ctr -n k8s.io images pull --user datawikipro:${GITHUB_TOKEN} ${IMAGE_NAME} || true"

echo -e "\033[0;32m  Deployment Complete!\033[0m"
