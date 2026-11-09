#!/usr/bin/env bash
# Posts usage at any node, asks the current owner to shut down gracefully,
# then shows that the customer moves and new events still land.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CUSTOMER="${CUSTOMER:-acme}"
ENTRY="${ENTRY:-http://127.0.0.1:8081}"
PORTS=(8081 8082 8083)

wait_healthy() {
  local port="$1"
  local i
  for i in $(seq 1 90); do
    if curl -fsS "http://127.0.0.1:${port}/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "node on :${port} did not become healthy" >&2
  return 1
}

route_json() {
  curl -fsS "${1}/v1/route?customer_id=${CUSTOMER}"
}

owner_id_from() {
  # {"customer_id":"acme","local":false,"owner_id":"node2","owner_url":"..."}
  printf '%s' "$1" | sed -n 's/.*"owner_id":"\([^"]*\)".*/\1/p'
}

port_for_node() {
  case "$1" in
    node1) echo 8081 ;;
    node2) echo 8082 ;;
    node3) echo 8083 ;;
    *)
      echo "unknown node id: $1" >&2
      return 1
      ;;
  esac
}

echo "bringing up the cluster"
docker compose up -d --build

for port in "${PORTS[@]}"; do
  wait_healthy "$port"
done

BEFORE="$(route_json "$ENTRY")"
OLD_OWNER="$(owner_id_from "$BEFORE")"
OWNER_PORT="$(port_for_node "$OLD_OWNER")"
echo "before: $BEFORE"

echo "posting usage through ${ENTRY}"
curl -fsS -X POST "${ENTRY}/v1/usage" \
  -H "Content-Type: application/json" \
  -d "{\"customer_id\":\"${CUSTOMER}\",\"metric\":\"tokens\",\"quantity\":10}" >/dev/null

echo "asking ${OLD_OWNER} on :${OWNER_PORT} to shut down gracefully"
curl -fsS -X POST "http://127.0.0.1:${OWNER_PORT}/v1/admin/exit" >/dev/null

NEW_OWNER="$OLD_OWNER"
AFTER=""
for _ in $(seq 1 40); do
  sleep 1
  if ! AFTER="$(route_json "$ENTRY" 2>/dev/null)"; then
    continue
  fi
  NEW_OWNER="$(owner_id_from "$AFTER")"
  if [[ -n "$NEW_OWNER" && "$NEW_OWNER" != "$OLD_OWNER" ]]; then
    break
  fi
done

echo "after:  $AFTER"
if [[ "$NEW_OWNER" == "$OLD_OWNER" || -z "$NEW_OWNER" ]]; then
  echo "failover failed: ${CUSTOMER} is still owned by ${OLD_OWNER}" >&2
  exit 1
fi

echo "posting again after failover"
curl -fsS -X POST "${ENTRY}/v1/usage" \
  -H "Content-Type: application/json" \
  -d "{\"customer_id\":\"${CUSTOMER}\",\"metric\":\"tokens\",\"quantity\":5}" >/dev/null

NEW_PORT="$(port_for_node "$NEW_OWNER")"
echo "new owner totals:"
curl -fsS "http://127.0.0.1:${NEW_PORT}/v1/totals"
echo
echo "failover ok: ${CUSTOMER} moved from ${OLD_OWNER} to ${NEW_OWNER}"
