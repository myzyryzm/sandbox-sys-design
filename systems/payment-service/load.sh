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
