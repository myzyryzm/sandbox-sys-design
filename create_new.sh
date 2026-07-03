#!/usr/bin/env bash
#
# Create a new, minimal sandbox system from scratch, then start it.
#
#   ./create_new.sh <new-system-id>
#   ./create_new.sh my-system
#
# Generates systems/<new-system-id>/ as the smallest runnable system:
#
#     nginx LB  ->  service-1 (generic FastAPI), scraped by Prometheus
#
# That's exactly the shape "Add service" creates — a clean service exposing
# /health + /metrics, no database, no downstream edges, no custom service types.
# The service code is COPIED from the canonical template the web app itself uses
# (frontend/server/templates/service/), so a fresh system and an Add-service node
# stay byte-identical. Everything else (compose, nginx, prometheus, manifest) is
# generated minimal here. From this clean base you grow the system in the UI
# (Add service / Add database / custom types) — no frontend edits needed.
#
# Immediately brings it up via start.sh (docker stack + shared frontend pointed
# at the new system). Only one system holds the shared host ports at a time.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SYSTEMS_DIR="$ROOT/systems"
# The canonical generic service (hand-instrumented FastAPI: /health + /metrics).
# This is the same template "Add service" clones, so service-1 here matches it.
TEMPLATE_DIR="$ROOT/frontend/server/templates/service"
SERVICE_FILES=(app.py requirements.txt Dockerfile)

usage() {
  echo "Usage: ./create_new.sh <new-system-id>" >&2
  echo "  Generates a minimal system (nginx LB + Prometheus + one generic service) and starts it." >&2
  echo "Existing systems:" >&2
  ls -1 "$SYSTEMS_DIR" 2>/dev/null | sed 's/^/  /' >&2
}

NEW_ID="${1:-}"
if [[ -z "$NEW_ID" || "$NEW_ID" == "-h" || "$NEW_ID" == "--help" ]]; then
  usage
  exit 1
fi

# Must be a safe folder + docker-compose project name.
if [[ ! "$NEW_ID" =~ ^[a-z0-9][a-z0-9-]*$ ]]; then
  echo "Invalid system id: '$NEW_ID'" >&2
  echo "Use lowercase letters, digits and hyphens, starting with a letter/digit." >&2
  exit 1
fi

NEW_DIR="$SYSTEMS_DIR/$NEW_ID"

if [[ -e "$NEW_DIR" ]]; then
  echo "System already exists: '$NEW_ID' ($NEW_DIR)" >&2
  echo "Start it with: ./start.sh $NEW_ID" >&2
  exit 1
fi
if [[ ! -d "$TEMPLATE_DIR" ]]; then
  echo "Service template not found: $TEMPLATE_DIR" >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required to generate the manifest." >&2
  exit 1
fi

# Human-readable display name: "my-system" -> "My System".
DISPLAY_NAME="$(echo "$NEW_ID" | tr '-' ' ' | awk '{ for (i=1; i<=NF; i++) $i = toupper(substr($i,1,1)) substr($i,2) } 1')"

echo "==> Generating minimal system '$NEW_ID' ($DISPLAY_NAME)…"
mkdir -p "$NEW_DIR/nginx" "$NEW_DIR/prometheus" "$NEW_DIR/service-1"

# --- service-1: copy the canonical generic FastAPI service (single source of truth) ---
for f in "${SERVICE_FILES[@]}"; do
  cp "$TEMPLATE_DIR/$f" "$NEW_DIR/service-1/$f"
done

# --- docker-compose.yml: lb + prometheus + service-1 (matches the Add-service shape) ---
cat >"$NEW_DIR/docker-compose.yml" <<COMPOSE
# Self-contained compose file for the \`$NEW_ID\` system.
# Run from inside systems/$NEW_ID/:  docker compose up --build
#
# Topology: nginx LB  ->  service-1 (generic FastAPI), with Prometheus scraping
# the service directly. service-1 is a generic service (the same shape "Add
# service" creates): it exposes /health and /metrics and is reached through the
# LB at the /service-1/ prefix. More services slot in the same way — each gets
# its own compose service, an nginx /<id>/ route, and a Prometheus scrape job.
#
# Restartable: \`docker compose down\` then \`up\` cleanly recreates everything.
# Prometheus data is intentionally NOT persisted.

services:
  lb:
    image: nginx:1.27-alpine
    ports:
      - "8080:80"
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/nginx.conf:ro

  prometheus:
    image: prom/prometheus:v3.1.0
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro

  service-1:
    # Service "service-1" — generic FastAPI backend (the same shape "Add service" creates)
    build: ./service-1
COMPOSE

# --- nginx/nginx.conf: one /service-1/ route, with the Add-service insertion markers ---
cat >"$NEW_DIR/nginx/nginx.conf" <<'NGINX'
# nginx as the load balancer / router in front of the services.
#
# Each service is reached at its own /<service-id>/ prefix, which nginx strips
# before proxying (the trailing slash on proxy_pass), so the browser can call
# e.g. /service-1/health and it lands on service-1's /health. nginx matches the
# longest prefix, so adding more services never collides.
#
# The "Add service" button inserts a new upstream and a new location at the
# markers below — keep the marker comments in place.

events {}

http {
    # === upstreams (one per service; add `server` lines for replicas) ===
    upstream service-1 { server service-1:8000; }
    # === end upstreams ===

    server {
        listen 80;

        # === locations (one per service) ===
        location /service-1/ {
            proxy_pass http://service-1/;
            proxy_set_header Host              $host;
            proxy_set_header X-Real-IP         $remote_addr;
            proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }
        # === end locations ===
    }
}
NGINX

# --- prometheus/prometheus.yml: scrape service-1 directly (Add service appends jobs) ---
cat >"$NEW_DIR/prometheus/prometheus.yml" <<'PROM'
global:
  scrape_interval: 5s
  evaluation_interval: 5s

scrape_configs:
  # Scrape each service container DIRECTLY (not through the nginx LB), so the
  # metrics reflect the service itself. `service-1:8000` resolves on the compose
  # network. The job name is the service id, which the manifest's health/metric
  # queries key off of: up{job="service-1"}. "Add service" appends a job here.
  [
    # Service "service-1" — generic FastAPI backend
    { job_name: service-1, static_configs: [ { targets: [ service-1:8000 ] } ] }
  ]
PROM

# --- endpoints.json: no public endpoints out of the box (just /health + /metrics) ---
cat >"$NEW_DIR/endpoints.json" <<'ENDPOINTS'
{}
ENDPOINTS

# --- load.sh: drive traffic through the LB so the metrics move ---
cat >"$NEW_DIR/load.sh" <<'LOAD'
#!/usr/bin/env bash
# Generate steady load through the nginx LB so metrics move.
# Usage: ./load.sh [delay-seconds-between-requests]   (Ctrl-C to stop)
#
# The target and HTTP method are overridable via env so the frontend's "Test"
# panel can drive a specific endpoint:
#   URL=http://localhost:8080/service-1/items METHOD=POST ./load.sh 0.05
set -euo pipefail

URL="${URL:-http://localhost:8080/service-1/health}"
METHOD="${METHOD:-GET}"
DELAY="${1:-0.05}"   # seconds between requests

echo "Hammering $METHOD $URL (delay ${DELAY}s). Ctrl-C to stop."
while true; do
  curl -s -o /dev/null -X "$METHOD" "$URL" || true
  sleep "$DELAY"
done
LOAD
chmod +x "$NEW_DIR/load.sh"

# --- manifest.json: lb + service-1 nodes, no edges. Metrics/health match scaffold.js ---
python3 - "$NEW_DIR/manifest.json" "$NEW_ID" "$DISPLAY_NAME" <<'PY'
import json, sys
path, system_id, name = sys.argv[1], sys.argv[2], sys.argv[3]


def service_metrics(svc):
    j = '{job="%s"}' % svc
    return [
        {"label": "req/s", "query": "sum(rate(http_requests_total%s[1m]))" % j, "unit": "/s"},
        {"label": "p95",
         "query": "histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket%s[1m])) by (le)) * 1000" % j,
         "unit": "ms"},
        {"label": "in-flight", "query": "sum(http_requests_in_flight%s)" % j, "unit": ""},
        {"label": "errors",
         "query": ('(sum(rate(http_requests_total{job="%s",status=~"5.."}[1m])) or vector(0)) '
                   '/ clamp_min(sum(rate(http_requests_total%s[1m])), 0.0001)') % (svc, j),
         "unit": "%", "scale": 100},
    ]


def service_health(svc):
    return {
        "query": 'up{job="%s"}' % svc,
        "rules": [
            {"color": "red", "when": "value < 1"},
            {"color": "green", "when": "value >= 1"},
        ],
    }


manifest = {
    "system_id": system_id,
    "name": name,
    "prometheus_base": "/api/prometheus",
    "poll_interval_ms": 4000,
    "nodes": [
        {
            "id": "lb",
            "label": "nginx LB",
            "type": "load_balancer",
            "position": {"x": 80, "y": 160},
            "metrics": [],
        },
        {
            "id": "service-1",
            "label": "service-1",
            "type": "service",
            "origin": "create-service",
            "position": {"x": 80, "y": 380},
            "metrics": service_metrics("service-1"),
            "health": service_health("service-1"),
        },
    ],
    "edges": [],
}
with open(path, "w") as f:
    json.dump(manifest, f, indent=2)
    f.write("\n")
PY

# Drop a per-system README placeholder so the new system has a home for its own
# notes and a checklist of what to change as it grows.
cat >"$NEW_DIR/README.md" <<EOF
# $DISPLAY_NAME

System id: \`$NEW_ID\` — generated by \`create_new.sh\` on $(date +%Y-%m-%d).

At creation this is the smallest runnable system: one nginx LB → \`service-1\`
(a generic FastAPI service exposing \`/health\` + \`/metrics\`), scraped by
Prometheus. No database, no downstream edges, no custom service types. Grow it
from the web app (Add service / Add database / custom types) or by editing the
files below — the shared frontend renders whatever the manifest describes, no
frontend edits needed.

## Run it

\`\`\`bash
./start.sh $NEW_ID      # from the repo root
./stop.sh  $NEW_ID
\`\`\`

Only one system holds the shared host ports (8080/9090) at a time, so starting
this one stops whichever system was previously active.

## What to change as this grows

- \`manifest.json\` — topology (\`nodes\`/\`edges\`), per-node \`metrics[]\` PromQL,
  and \`health\` rules. This is what the diagram renders.
- \`service-1/app.py\` — the service logic and the hand-written metrics it exposes.
- \`docker-compose.yml\` — add services (replicas, a DB, a cache, exporters…).
- \`nginx/nginx.conf\` — per-service \`/<id>/\` routes and \`upstream\` blocks.
- \`prometheus/prometheus.yml\` — scrape targets for any new services.
- \`load.sh\` — how the smoke test drives traffic.
EOF

echo "==> Created systems/$NEW_ID (\"$DISPLAY_NAME\")"
echo "==> Starting it…"
echo

exec "$ROOT/start.sh" "$NEW_ID"
