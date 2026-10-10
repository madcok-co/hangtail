#!/usr/bin/env bash
# record_tail_demo.sh — "hangtail tail" without any cloud exporter.
#
# Usage (from project root):
#   asciinema rec -t "hangtail tail — zero-exporter debugging" \
#     --cols 200 --rows 50 --overwrite \
#     demo/hangtail-tail-demo.cast \
#     --command "bash demo/record_tail_demo.sh"

set -euo pipefail

ROOT="/home/madcok/documents/projects/hangtail"

# Kill any leftover session and clean up volumes so each recording is fresh
tmux kill-session -t htdemo 2>/dev/null || true
docker compose -f "$ROOT/demo/docker-compose-tail.yml" down -v 2>/dev/null || true
sleep 0.3

cat > /tmp/htdemo_first_pane.sh << 'FIRST'
#!/usr/bin/env bash
SESSION="htdemo"
ROOT="/home/madcok/documents/projects/hangtail"
COMPOSE="docker compose -f $ROOT/demo/docker-compose-tail.yml"
C='\033[36;1m'
G='\033[32;1m'
R='\033[0m'

# ── Wait for a client to attach before splitting ──────────────────────────
# tmux split-window needs a known window size. Before any client attaches the
# window is "size missing" even if new-session was given -x/-y.
until [[ "$(tmux display-message -t "$SESSION" -p '#{session_attached}' 2>/dev/null)" != "0" ]]; do
    sleep 0.05
done
sleep 0.3   # let the PTY size propagate to tmux

# ── Split: left 65% (tail output) · right 35% (commentary / commands) ────
tmux split-window -h -t "$SESSION:0" -p 35
sleep 0.2

# Pane helpers
L()    { tmux send-keys -t "$SESSION:0.0" "$@"; }
_R()   { tmux send-keys -t "$SESSION:0.1" "$@"; }
lrun() { L "$*" Enter; }
rrun() { _R "$*" Enter; }
rsay() {
    _R "" Enter; sleep 0.1
    rrun "printf '${C}# $*${R}\\n'"
    sleep 0.7
}

# ── Background driver ─────────────────────────────────────────────────────
(
    sleep 0.5

    # Title
    rrun "clear"
    sleep 0.3
    rrun "printf '${G}  hangtail tail\\n  debug failed flows — no cloud exporter${R}\\n'"
    sleep 2.5

    # ── step 1: show config ───────────────────────────────────────────────
    rsay "step 1 — collector config: no Loki, no cloud"
    rrun "cat $ROOT/demo/collector/config-tail-only.yaml"
    sleep 5.5

    # ── step 2: start stack ───────────────────────────────────────────────
    rsay "step 2 — start the stack"
    rrun "$COMPOSE up -d 2>&1 | grep -ivE 'warning|obsolete|network'"
    sleep 3

    # driver-side waits (silent — no terminal noise)
    until curl -sf http://localhost:8080/healthz > /dev/null 2>&1; do sleep 2; done
    until nc -z localhost 4320 2>/dev/null; do sleep 1; done
    sleep 2

    # ── step 3: connect tail ──────────────────────────────────────────────
    rsay "step 3 — stream forwarded flows (left pane)"
    sleep 0.5
    lrun "$ROOT/hangtail tail --addr localhost:4320"
    sleep 1.5

    # ── step 4: send orders ───────────────────────────────────────────────
    rsay "step 4 — 15 orders at 40% fail rate"
    sleep 0.5
    rrun "for i in \$(seq 1 15); do curl -sX POST localhost:8080/order -H 'Content-Type: application/json' -d \"{\\\"item\\\":\\\"item-\$i\\\",\\\"amount\\\":100}\"; echo; sleep 0.25; done"
    sleep 10

    # ── step 5: wait for reaper (segment_max_age = 30s) ──────────────────
    rsay "segment_max_age=30s — reaper waking up ..."
    sleep 32

    rsay "← failed flows in real time, zero cloud exporters"
    sleep 5.0

    # ── step 6: --level filter ────────────────────────────────────────────
    rsay "step 5 — filter: ERROR records only  (--level error)"
    L "C-c"
    sleep 0.8
    lrun "$ROOT/hangtail tail --addr localhost:4320 --level error"
    sleep 1.0

    rsay "second wave — 10 more orders"
    rrun "for i in \$(seq 16 25); do curl -sX POST localhost:8080/order -H 'Content-Type: application/json' -d \"{\\\"item\\\":\\\"item-\$i\\\",\\\"amount\\\":100}\"; echo; sleep 0.2; done"
    sleep 10
    rsay "reaper ..."
    sleep 32

    rsay "← only ERROR lines with --level error"
    sleep 4.0

    # ── step 7: --service filter ──────────────────────────────────────────
    rsay "step 6 — filter: worker-py only  (--service worker)"
    L "C-c"
    sleep 0.8
    lrun "$ROOT/hangtail tail --addr localhost:4320 --service worker"
    sleep 1.0

    rrun "for i in \$(seq 26 30); do curl -sX POST localhost:8080/order -H 'Content-Type: application/json' -d \"{\\\"item\\\":\\\"item-\$i\\\",\\\"amount\\\":100}\"; echo; sleep 0.2; done"
    sleep 8
    rsay "reaper ..."
    sleep 33

    rsay "← only worker-py flows with --service worker"
    sleep 4.0

    # ── outro ─────────────────────────────────────────────────────────────
    L "C-c"
    sleep 0.5
    rrun "printf '\\n${G}'"
    rrun "printf '  ✓  failed flows in real time via SSE\\n'"
    rrun "printf '  ✓  full log body + attrs (ts / severity / service)\\n'"
    rrun "printf '  ✓  --level and --service filters\\n'"
    rrun "printf '  ✓  disk-durable: survives collector restart\\n'"
    rrun "printf '  github.com/madcok/hangtail${R}\\n'"
    sleep 5

    rrun "exit"
    sleep 1.5
    tmux kill-session -t "$SESSION" 2>/dev/null || true
) &

# Keep pane 0 alive — driver sends commands here via send-keys
exec bash
FIRST

chmod +x /tmp/htdemo_first_pane.sh

tmux new-session -d -s htdemo -x 200 -y 50 "bash /tmp/htdemo_first_pane.sh"
tmux attach-session -t htdemo || true
