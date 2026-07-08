#!/usr/bin/env bash
#
# Start a sandbox system by id.
#
#   ./start.sh <system-id> [--no-frontend]
#   ./start.sh hello-lb
#
# Brings up the system's docker compose stack (nginx LB, backend, Prometheus)
# and, unless --no-frontend is given, starts the shared frontend dev server
# pointed at that system (VITE_SYSTEM_ID=<system-id>).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FRONTEND_DIR="$ROOT/frontend"
RUN_DIR="$ROOT/.run"

usage() {
  echo "Usage: ./start.sh <system-id> [--no-frontend]" >&2
  echo "Available systems:" >&2
  ls -1 "$ROOT/systems" 2>/dev/null | sed 's/^/  /' >&2
}

SYSTEM_ID="${1:-}"
if [[ -z "$SYSTEM_ID" || "$SYSTEM_ID" == "-h" || "$SYSTEM_ID" == "--help" ]]; then
  usage
  exit 1
fi

SYSTEM_DIR="$ROOT/systems/$SYSTEM_ID"
COMPOSE_FILE="$SYSTEM_DIR/docker-compose.yml"
if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "No such system: '$SYSTEM_ID' (expected $COMPOSE_FILE)" >&2
  usage
  exit 1
fi

START_FRONTEND=1
[[ "${2:-}" == "--no-frontend" ]] && START_FRONTEND=0

# Only one system can hold the shared host ports (8000/8080/9090) at a time.
# If a different system is currently active, bring its stack down first so the
# new one can claim the ports.
ACTIVE_FILE="$RUN_DIR/active_system"
if [[ -f "$ACTIVE_FILE" ]]; then
  PREV_ID="$(cat "$ACTIVE_FILE" 2>/dev/null || true)"
  PREV_COMPOSE="$ROOT/systems/$PREV_ID/docker-compose.yml"
  if [[ -n "$PREV_ID" && "$PREV_ID" != "$SYSTEM_ID" && -f "$PREV_COMPOSE" ]]; then
    echo "==> Stopping previously active system '$PREV_ID' (frees shared ports)…"
    docker compose -f "$PREV_COMPOSE" down || true
  fi
fi

echo "==> Starting system '$SYSTEM_ID' (docker compose up --build -d)…"
docker compose -f "$COMPOSE_FILE" up --build -d
echo
docker compose -f "$COMPOSE_FILE" ps

# Record the active system regardless of the frontend flag, so a later start
# knows which stack to bring down before claiming the shared ports.
mkdir -p "$RUN_DIR"
echo "$SYSTEM_ID" >"$RUN_DIR/active_system"

if [[ "$START_FRONTEND" == "1" ]]; then
  PID_FILE="$RUN_DIR/frontend.pid"
  LOG_FILE="$RUN_DIR/frontend.log"

  # Stop any frontend dev server we previously started (single shared frontend).
  if [[ -f "$PID_FILE" ]]; then
    kill "$(cat "$PID_FILE")" 2>/dev/null || true
  fi
  pkill -f "$FRONTEND_DIR/node_modules/.*vite" 2>/dev/null || true
  sleep 1

  if [[ ! -d "$FRONTEND_DIR/node_modules" ]]; then
    echo "==> Installing frontend dependencies (first run)…"
    ( cd "$FRONTEND_DIR" && npm install )
  fi

  echo "==> Starting frontend (VITE_SYSTEM_ID=$SYSTEM_ID)…"
  ( cd "$FRONTEND_DIR" && VITE_SYSTEM_ID="$SYSTEM_ID" exec npm run dev ) >"$LOG_FILE" 2>&1 &
  echo $! >"$PID_FILE"
  sleep 2
fi

echo
echo "Ready — system '$SYSTEM_ID':"
[[ "$START_FRONTEND" == "1" ]] && echo "  Frontend (diagram) : http://localhost:5173"
echo "  Load balancer      : http://localhost:8080/health"
echo "  Prometheus         : http://localhost:9090  (targets: /targets)"
echo
echo "  Stop          : ./stop.sh $SYSTEM_ID"
