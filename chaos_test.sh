#!/usr/bin/env bash
# chaos_test.sh — Verifies hangtail's default-keep guarantee under collector crashes.
#
# Algorithm:
#   1. Start the full docker-compose stack.
#   2. Send N requests to the demo API; record which order IDs received HTTP errors
#      (those are flows that never sent flow.end).
#   3. Kill the collector mid-run to simulate a crash.
#   4. Restart the collector; wait for the reaper to flush pending segments.
#   5. Query Loki for all order_id values present in forwarded logs.
#   6. ASSERT: every failed order_id is in Loki; no successful order_id is in Loki.
#
# Requirements: docker compose, curl, python3

set -euo pipefail

API_URL="http://localhost:8080/order"
LOKI_URL="http://localhost:3100"
REQUESTS=200
CONCURRENCY=10
SEGMENT_MAX_AGE_SEC=130   # 2m + 10s margin

FAILED_IDS_FILE=$(mktemp)
SUCCESS_IDS_FILE=$(mktemp)
trap 'rm -f "$FAILED_IDS_FILE" "$SUCCESS_IDS_FILE"' EXIT

log() { echo "[$(date +%H:%M:%S)] $*"; }

# ── 1. Start stack ────────────────────────────────────────────────────────────
log "Starting docker compose stack..."
docker compose -f demo/docker-compose.yml up -d --build --wait --wait-timeout 120

log "Waiting for API to be ready..."
for i in $(seq 1 30); do
  if curl -sf "http://localhost:8080/healthz" > /dev/null 2>&1; then
    break
  fi
  sleep 2
done

# ── 2. Send requests, record failures ────────────────────────────────────────
log "Sending $REQUESTS requests (concurrency=$CONCURRENCY)..."

send_batch() {
  for _ in $(seq 1 $((REQUESTS / CONCURRENCY))); do
    response=$(curl -s -w '\n%{http_code}' "$API_URL" 2>/dev/null)
    body=$(head -1 <<< "$response")
    code=$(tail -1 <<< "$response")
    order_id=$(python3 -c "import sys,json; d=json.loads(sys.argv[1]); print(d.get('order_id',''))" "$body" 2>/dev/null) || true
    if [[ -n "$order_id" ]]; then
      if [[ "$code" == "200" ]]; then
        echo "$order_id" >> "$SUCCESS_IDS_FILE"
      else
        echo "$order_id" >> "$FAILED_IDS_FILE"
      fi
    fi
  done
}

pids=()
for _ in $(seq 1 $CONCURRENCY); do
  send_batch &
  pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid"; done

FAILED_COUNT=$(wc -l < "$FAILED_IDS_FILE" | tr -d ' ')
SUCCESS_COUNT=$(wc -l < "$SUCCESS_IDS_FILE" | tr -d ' ')
log "Requests done. Success=$SUCCESS_COUNT, Failed=$FAILED_COUNT"

# ── 3. Kill the collector ─────────────────────────────────────────────────────
# Wait for the OTel SDK batch processor to flush (default batch timeout ~5s).
# This simulates a crash that happens after logs reach the collector but before
# the collector's reaper would normally fire.
log "Waiting 8s for OTel SDK to flush batches to collector..."
sleep 8

log "Killing collector to simulate crash..."
docker compose -f demo/docker-compose.yml kill collector
sleep 2

# ── 4. Restart collector and wait for reap ────────────────────────────────────
log "Restarting collector..."
docker compose -f demo/docker-compose.yml start collector

log "Waiting ${SEGMENT_MAX_AGE_SEC}s for reaper to flush pending segments..."
sleep "$SEGMENT_MAX_AGE_SEC"

# ── 5. Query Loki ─────────────────────────────────────────────────────────────
log "Querying Loki for forwarded order IDs..."

LOKI_QUERY='{service_name="api-go"}'
LOKI_RESPONSE=$(curl -sG "$LOKI_URL/loki/api/v1/query_range" \
  --data-urlencode "query=$LOKI_QUERY" \
  --data-urlencode "start=$(date -d '1 hour ago' +%s)000000000" \
  --data-urlencode "end=$(date +%s)000000000" \
  --data-urlencode "limit=5000")

# order_id is stored as a Loki stream label, not in the log body
LOKI_IDS=$(echo "$LOKI_RESPONSE" | python3 -c "
import sys, json
d = json.load(sys.stdin)
ids = set()
for stream in d['data']['result']:
    oid = stream['stream'].get('order_id', '')
    if oid:
        ids.add(oid)
print('\n'.join(sorted(ids)))
")

# ── 6. Assert ─────────────────────────────────────────────────────────────────
ERRORS=0

log "Checking: all failed order IDs should appear in Loki..."
while IFS= read -r oid; do
  if ! echo "$LOKI_IDS" | grep -qF "$oid"; then
    log "  FAIL: failed order $oid NOT found in Loki (should have been forwarded)"
    ((ERRORS++))
  fi
done < "$FAILED_IDS_FILE"

log "Checking: successful order IDs should NOT appear in Loki..."
while IFS= read -r oid; do
  if echo "$LOKI_IDS" | grep -qF "$oid"; then
    log "  FAIL: successful order $oid FOUND in Loki (should have been dropped)"
    ((ERRORS++))
  fi
done < "$SUCCESS_IDS_FILE"

echo ""
if [[ $ERRORS -eq 0 ]]; then
  log "✓ PASS — hangtail correctly forwarded all $FAILED_COUNT failed flows and dropped all $SUCCESS_COUNT completed flows."
  EXIT_CODE=0
else
  log "✗ FAIL — $ERRORS assertion(s) failed. See output above."
  EXIT_CODE=1
fi

docker compose -f demo/docker-compose.yml down -v
exit $EXIT_CODE
