#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "Stopping TikBlog and deleting persistent data..."

docker compose \
  -f infra/docker-compose.yml \
  -f infra/docker-compose.analytics.yml \
  down \
  -v \
  --remove-orphans

echo "TikBlog local environment has been reset."
echo "Kafka messages, Delta tables, checkpoints, Postgres and local app metadata are gone."