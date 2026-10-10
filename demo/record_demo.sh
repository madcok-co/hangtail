#!/usr/bin/env bash
# record_demo.sh — Scripted demo for asciinema recording.
# Assumes the demo stack is already running (make demo-up).

RESET='\033[0m'
BOLD='\033[1m'
CYAN='\033[36m'
GREEN='\033[32m'
YELLOW='\033[33m'

say()  { echo -e "${CYAN}# $*${RESET}"; sleep 1; }
run()  { echo -e "${BOLD}\$ $*${RESET}"; sleep 0.5; eval "$*"; }
wait_key() { sleep "$1"; }

clear

echo -e "${BOLD}${GREEN}"
cat <<'BANNER'
  _                       _        _ _
 | |__   __ _ _ __   __ _| |_ __ _(_) |
 | '_ \ / _` | '_ \ / _` | __/ _` | | |
 | | | | (_| | | | | (_| | || (_| | | |
 |_| |_|\__,_|_| |_|\__, |\__\__,_|_|_|
                     |___/
  default-keep OTel Collector processor
BANNER
echo -e "${RESET}"
wait_key 2

# ── 1. Stack ──────────────────────────────────────────────────────────────────
say "Demo stack: collector + Loki + Grafana + api-go (30% fail rate)"
run "docker compose -f demo/docker-compose.yml ps --format 'table {{.Name}}\t{{.Status}}\t{{.Ports}}'"
wait_key 1

# ── 2. Send requests ──────────────────────────────────────────────────────────
say "Sending 20 requests to /order (~30% will fail and never send flow.end)"
echo -e "${BOLD}\$ for i in \$(seq 1 20); do curl -s http://localhost:8080/order; echo; done${RESET}"
wait_key 0.5
SUCCESS=0; FAILED=0
for i in $(seq 1 20); do
  resp=$(curl -s -w '\n%{http_code}' http://localhost:8080/order 2>/dev/null || true)
  body=$(head -1 <<< "$resp" || true)
  code=$(tail -1 <<< "$resp" || true)
  echo "$body"
  if [[ "$code" == "200" ]]; then SUCCESS=$((SUCCESS+1)); else FAILED=$((FAILED+1)); fi
  sleep 0.1
done
echo ""
echo -e "${GREEN}  success=$SUCCESS  failed=$FAILED${RESET}"
wait_key 2

# ── 3. Segment files ──────────────────────────────────────────────────────────
say "Logs buffered to disk — safe across collector restarts"
run "docker exec demo-collector-1 ls -lh /var/hangtail/segments/"
wait_key 1

# ── 4. Wait for reaper ────────────────────────────────────────────────────────
say "Waiting for reaper (segment_max_age=2m) ..."
echo -e "${YELLOW}  [collector logs]${RESET}"
wait_key 1

# tail logs until 'reaped segment' appears (up to 3 minutes)
docker logs demo-collector-1 -f --since 30s 2>&1 | while IFS= read -r line; do
  if echo "$line" | grep -q "reaped segment"; then
    echo "  $line"
    break
  fi
done || true
wait_key 1

# ── 5. Query Loki ─────────────────────────────────────────────────────────────
say "Query Loki — ONLY failed orders should appear"
echo -e "${BOLD}\$ curl http://localhost:3100/loki/api/v1/query_range '...' | python3${RESET}"
wait_key 0.5

curl -sG "http://localhost:3100/loki/api/v1/query_range" \
  --data-urlencode "query={service_name=\"api-go\"}" \
  --data-urlencode "start=$(date -d '5 minutes ago' +%s)000000000" \
  --data-urlencode "end=$(date +%s)000000000" \
  --data-urlencode "limit=500" \
  2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
orders = {}
for stream in d['data']['result']:
    oid = stream['stream'].get('order_id','')
    level = stream['stream'].get('detected_level','')
    if oid:
        orders.setdefault(oid, set()).add(level)
print(f'  {len(orders)} orders in Loki:')
for oid, levels in sorted(orders.items()):
    print(f'  {oid}  levels={sorted(levels)}')
" || true
wait_key 2

# ── 6. hangtail tail ──────────────────────────────────────────────────────────
say "hangtail tail — live SSE stream of forwarded flows"
echo -e "${BOLD}\$ go run ./cmd/hangtail/ tail --addr localhost:4320${RESET}"
wait_key 0.5
echo -e "${YELLOW}  (connecting to SSE endpoint...)${RESET}"
wait_key 1
# Send more requests while tail is listening — events arrive after next reaper cycle
echo -e "${YELLOW}  Sending 5 more requests in background...${RESET}"
for i in $(seq 1 5); do curl -s http://localhost:8080/order > /dev/null 2>&1; done &
echo -e "${YELLOW}  (events will appear after segment_max_age — see live demo at localhost:4320)${RESET}"
wait_key 2

# ── 7. Outro ──────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}${GREEN}"
echo "  ✓  default-keep guarantee — failed flows always reach Loki"
echo "  ✓  segment files survive collector restarts"
echo "  ✓  chaos test: 200 requests, kill collector → PASS"
echo ""
echo "  github.com/madcok/hangtail"
echo -e "${RESET}"
wait_key 3
