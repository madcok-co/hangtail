#!/usr/bin/env bash
# chaos_test.sh — Verifies hangtail's default-keep guarantee under collector crashes.
#
# Algorithm:
#   1. Start the full docker-compose stack with FAIL_RATE=0 (all flows succeed).
#   2. Send N requests to the demo API; record order IDs.
#   3. Wait for processing queues to drain + OTel SDK to flush all batches.
#   4. Kill the collector to simulate a crash.
#   5. Restart the collector; wait for the reaper to flush pending segments.
#   6. Query Loki for all order_id values present in forwarded logs.
#   7. ASSERT: no order_id appears in Loki (all flows completed, none should be forwarded).
#
# FAIL_RATE is set to 0 so every flow sends flow.end=true. This isolates the
# crash-durability guarantee: the only orders that could appear in Loki are those
# whose flow.end signal was lost due to the collector crash.
#
# Requirements: docker compose, curl, python3

set -euo pipefail

API_URL="http://localhost:8080/order"
LOKI_URL="http://localhost:3100"
REQUESTS=200
CONCURRENCY=10
SEGMENT_MAX_AGE_SEC=130   # 2m + 10s margin

# Run with FAIL_RATE=0 so all flows complete — isolates crash-durability, not failure detection.
export WORKER_FAIL_RATE=0
export NOTIFIER_FAIL_RATE=0

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
# Wait for both processing queues (worker + notifier) to drain before waiting
# for the OTel SDK to flush. Worker and notifier are single-threaded and process
# sequentially, so the last flow.end is emitted only after both queues are empty.
log "Waiting for Redis queues to drain (worker + notifier)..."
for _ in $(seq 1 60); do
  orders=$(docker compose -f demo/docker-compose.yml exec -T redis redis-cli llen orders:queue 2>/dev/null | tr -d '[:space:]')
  notifs=$(docker compose -f demo/docker-compose.yml exec -T redis redis-cli llen notifications:queue 2>/dev/null | tr -d '[:space:]')
  if [[ "$orders" == "0" && "$notifs" == "0" ]]; then
    log "Queues empty (orders=$orders, notifications=$notifs)."
    break
  fi
  sleep 1
done

# Now wait for all OTel SDK batch processors to flush their remaining buffers.
# Python and Node.js SDK default schedule_delay is 5s; add extra margin.
log "Waiting 10s for OTel SDK to flush remaining batches..."
sleep 10

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
# With FAIL_RATE=0, every flow sent flow.end=true. After the crash+recovery,
# hangtail should have discarded all completed flows. Any order_id found in Loki
# means a flow.end signal was lost due to the crash — a durability violation.
ERRORS=0

log "Checking: no completed order IDs should appear in Loki after crash+recovery..."
while IFS= read -r oid; do
  if echo "$LOKI_IDS" | grep -qF "$oid"; then
    log "  FAIL: successful order $oid FOUND in Loki (should have been dropped)"
    ((ERRORS++))
  fi
done < "$SUCCESS_IDS_FILE"

echo ""
if [[ $ERRORS -eq 0 ]]; then
  log "✓ PASS — hangtail correctly dropped all $SUCCESS_COUNT completed flows after crash+recovery."
  EXIT_CODE=0
else
  log "✗ FAIL — $ERRORS assertion(s) failed. See output above."
  EXIT_CODE=1
fi

docker compose -f demo/docker-compose.yml down -v
exit $EXIT_CODE
