#!/usr/bin/env bash
# run_fuzz_only.sh — Re-run just the two fuzz scenarios (bench logs already exist).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"

MADKV="$HOME/madkv"
FUZZER="$MADKV/target/release/fuzzer"
mkdir -p /tmp/madkv-p3/fuzz

GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[0;33m'; RED='\033[0;31m'; RESET='\033[0m'
log()  { echo -e "${CYAN}[$(date +%H:%M:%S)] $*${RESET}"; }
ok()   { echo -e "${GREEN}  ✓ $*${RESET}"; }
warn() { echo -e "${YELLOW}  ⚠ $*${RESET}"; }

ssh_n() {
  local host="$1"; shift
  ssh -o StrictHostKeyChecking=no -o ConnectTimeout=8 "$host" \
    "export PATH=\$HOME/.cargo/bin:\$HOME/.local/bin:\$PATH; $*" 2>/dev/null
}

start_cluster() {
  log "  Starting cluster RF=5 NPARTS=2 (clean)..."
  bash "${SCRIPT_DIR}/start_all.sh" --clean --rf 5 --nparts 2 \
    > /tmp/madkv-p3/start_cluster.log 2>&1
  source "${SCRIPT_DIR}/config.sh"
  log "  Waiting 12s for Raft elections to settle..."
  sleep 12
  # Quick sanity check
  log "  Manager: ${MANAGERS}"
}

stop_cluster() {
  bash "${SCRIPT_DIR}/stop_all.sh" > /dev/null 2>&1
  sleep 3
}

find_leader() {
  local part="$1" best_term=-1
  P_LEADER_HOST=""
  for ((r=0; r<RF; r++)); do
    local nidx=$(( part * RF + r + 2 ))
    local host="${NODE_HOSTS[$nidx]}"
    local tag="s${part}.${r}"
    local logf="${LOG_DIR}/server-${tag}.log"
    local term
    term=$(ssh_n "$host" \
      "grep 'became LEADER' '$logf' 2>/dev/null | grep -oE 'term=[0-9]+' | grep -oE '[0-9]+' | sort -n | tail -1" \
      2>/dev/null || true)
    term=${term//[^0-9]/}
    [[ -z "$term" || "$term" == "0" ]] && continue
    if (( term > best_term )); then
      best_term=$term; P_LEADER_HOST="$host"
    fi
  done
}

# ── FUZZ 1: RF=5, no crashes ─────────────────────────────────────────────────
log "════════════════════════════════════════════"
log "FUZZ 1 — RF=5, no crashes"
log "════════════════════════════════════════════"
start_cluster
FLOG="/tmp/madkv-p3/fuzz/fuzz-5-no.log"
log "  Running fuzzer (5 clients × 1000 ops)..."
cd "$MADKV"
"$FUZZER" \
  --num-clis 5 \
  --num-ops  1000 \
  --conflict \
  --client-just-args p3::client "${MANAGERS}" \
  > "$FLOG" 2>&1
FUZZ1_RC=$?
FUZZ1=$(grep -oE 'Fuzz testing result: \S+' "$FLOG" 2>/dev/null | tail -1 \
        || grep -o 'PASSED\|FAILED' "$FLOG" 2>/dev/null | tail -1 \
        || echo "exit=${FUZZ1_RC}")
ok "Fuzz-5-no => ${FUZZ1}"
stop_cluster

# ── FUZZ 2: RF=5, crash leader + 1 follower ──────────────────────────────────
log "════════════════════════════════════════════"
log "FUZZ 2 — RF=5, crash leader + follower"
log "════════════════════════════════════════════"
start_cluster
FLOG2="/tmp/madkv-p3/fuzz/fuzz-5-yes.log"
log "  Starting fuzzer in background (200 ops/client = 1000 total)..."
cd "$MADKV"
"$FUZZER" \
  --num-clis 5 \
  --num-ops  200 \
  --conflict \
  --client-just-args p3::client "${MANAGERS}" \
  > "$FLOG2" 2>&1 &
FUZZ_PID=$!

log "  Fuzzer PID=${FUZZ_PID}; waiting 20s before injecting crash..."
sleep 20

log "  Finding p0 leader..."
find_leader 0
if [[ -n "$P_LEADER_HOST" ]]; then
  log "  Killing leader on ${P_LEADER_HOST}..."
  ssh_n "$P_LEADER_HOST" "pkill -9 kvserver 2>/dev/null; true"
  ok "  Leader killed"
else
  warn "  No leader found; killing node2 as fallback..."
  ssh_n "${NODE_HOSTS[2]}" "pkill -9 kvserver 2>/dev/null; true"
fi

log "  Waiting 10s for new election..."
sleep 10

log "  Killing a p0 follower..."
for nidx in 3 4 5 6; do
  FHOST="${NODE_HOSTS[$nidx]}"
  if [[ "$FHOST" != "${P_LEADER_HOST:-NONE}" ]]; then
    ssh_n "$FHOST" "pkill -9 kvserver 2>/dev/null; true"
    ok "  Follower killed on ${FHOST}"
    break
  fi
done

log "  Waiting for fuzzer to complete (max 600s)..."
WAITED=0
while kill -0 "$FUZZ_PID" 2>/dev/null && (( WAITED < 600 )); do
  sleep 5; WAITED=$((WAITED + 5))
done
if kill -0 "$FUZZ_PID" 2>/dev/null; then
  warn "  Fuzzer timed out; terminating..."
  kill "$FUZZ_PID" 2>/dev/null || true
fi
wait "$FUZZ_PID" 2>/dev/null || true

FUZZ2=$(grep -oE 'Fuzz testing result: \S+' "$FLOG2" 2>/dev/null | tail -1 \
        || grep -o 'PASSED\|FAILED' "$FLOG2" 2>/dev/null | tail -1 \
        || echo "incomplete")
ok "Fuzz-5-yes => ${FUZZ2}"
stop_cluster

log "════════════════════════════════════════════"
log "FUZZ COMPLETE"
log "  fuzz-5-no  : ${FUZZ1}"
log "  fuzz-5-yes : ${FUZZ2}"
log "  logs at /tmp/madkv-p3/fuzz/"
log "════════════════════════════════════════════"
