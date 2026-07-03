#!/usr/bin/env bash
#
# Stop a sandbox system by id.
#
#   ./stop.sh <system-id> [--keep-frontend]
#   ./stop.sh hello-lb
#
# Tears down the system's docker compose stack and stops the shared frontend
# dev server (unless --keep-frontend is given). Mirrors start.sh.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FRONTEND_DIR="$ROOT/frontend"
RUN_DIR="$ROOT/.run"

usage() {
  echo "Usage: ./stop.sh <system-id> [--keep-frontend]" >&2
  echo "Available systems:" >&2
  ls -1 "$ROOT/systems" 2>/dev/null | sed 's/^/  /' >&2
}

SYSTEM_ID="${1:-}"
if [[ -z "$SYSTEM_ID" || "$SYSTEM_ID" == "-h" || "$SYSTEM_ID" == "--help" ]]; then
  usage
  exit 1
fi

COMPOSE_FILE="$ROOT/systems/$SYSTEM_ID/docker-compose.yml"
if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "No such system: '$SYSTEM_ID' (expected $COMPOSE_FILE)" >&2
  usage
  exit 1
fi

STOP_FRONTEND=1
[[ "${2:-}" == "--keep-frontend" ]] && STOP_FRONTEND=0

echo "==> Stopping system '$SYSTEM_ID' (docker compose down)…"
docker compose -f "$COMPOSE_FILE" down

if [[ "$STOP_FRONTEND" == "1" ]]; then
  echo "==> Stopping frontend dev server…"
  if [[ -f "$RUN_DIR/frontend.pid" ]]; then
    kill "$(cat "$RUN_DIR/frontend.pid")" 2>/dev/null || true
    rm -f "$RUN_DIR/frontend.pid"
  fi
  # Catch the vite child process too (npm spawns it).
  pkill -f "$FRONTEND_DIR/node_modules/.*vite" 2>/dev/null || true
  rm -f "$RUN_DIR/active_system"
fi

echo "Done."
