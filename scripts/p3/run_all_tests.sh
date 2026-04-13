#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run_all_tests.sh — Runs all fuzz + YCSB bench scenarios on CloudLab node0.
#
# Run from node0:  bash ~/madkv/scripts/p3/run_all_tests.sh
#
# Outputs:
#   /tmp/madkv-p3/fuzz/fuzz-5-no.log
#   /tmp/madkv-p3/fuzz/fuzz-5-yes.log
#   /tmp/madkv-p3/bench/bench-<nclis>-<wload>-<rf>.log  (24 files)
# ─────────────────────────────────────────────────────────────────────────────
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
MADKV="$HOME/madkv"
FUZZER="$MADKV/target/release/fuzzer"
BENCHER="$MADKV/target/release/bencher"
LOG_MASTER="/tmp/madkv-p3/run_all_tests.log"
mkdir -p /tmp/madkv-p3/fuzz /tmp/madkv-p3/bench
: > "$LOG_MASTER"

GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[0;33m'; RED='\033[0;31m'; RESET='\033[0m'
log()  { echo -e "${CYAN}[$(date +%H:%M:%S)] $*${RESET}" | tee -a "$LOG_MASTER"; }
ok()   { echo -e "${GREEN}  ✓ $*${RESET}" | tee -a "$LOG_MASTER"; }
warn() { echo -e "${YELLOW}  ⚠ $*${RESET}" | tee -a "$LOG_MASTER"; }
err()  { echo -e "${RED}  ✗ $*${RESET}" | tee -a "$LOG_MASTER"; }

# ── Cluster control ───────────────────────────────────────────────────────────
start_cluster() {
  local rf="$1" nparts="${2:-1}"
  log "  Starting cluster RF=${rf} NPARTS=${nparts}..."
  bash "${SCRIPT_DIR}/start_all.sh" --clean --rf "$rf" --nparts "$nparts" \
    > /tmp/madkv-p3/start_cluster.log 2>&1
  # Re-source to get updated MANAGERS/SERVERS for this RF
  source "${SCRIPT_DIR}/config.sh"
  sleep 2
}

stop_cluster() {
  bash "${SCRIPT_DIR}/stop_all.sh" > /dev/null 2>&1
  sleep 2
}

ssh_n() {
  local host="$1"; shift
  ssh -o StrictHostKeyChecking=no -o ConnectTimeout=8 "$host" \
    "export PATH=\$HOME/.cargo/bin:\$HOME/.local/bin:\$PATH; $*" 2>/dev/null
}

# ── Find Raft leader of a partition ─────────────────────────────────────────
# Sets P_LEADER_HOST
find_leader() {
  local part="$1"
  local best_term=-1
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
      best_term=$term
      P_LEADER_HOST="$host"
    fi
  done
}

# ═══════════════════════════════════════════════════════════════════════════════
# FUZZ TEST 1: RF=5, no crashes
# ═══════════════════════════════════════════════════════════════════════════════
log "════════════════════════════════════════"
log "FUZZ 1: RF=5, no crashing servers"
log "════════════════════════════════════════"

# Use config defaults (NPARTS=2, RF=5) so the manager knows about all 10 servers
start_cluster 5 2
source "${SCRIPT_DIR}/config.sh"
# Wait for Raft elections to stabilise before fuzzer connects
log "  Waiting 10s for Raft elections..."
sleep 10

FLOG="/tmp/madkv-p3/fuzz/fuzz-5-no.log"
log "  Running fuzzer → ${FLOG}"
cd "$MADKV"
"$FUZZER" \
  --num-clis 5 \
  --num-ops 1000 \
  --conflict \
  --client-just-args p3::client "${MANAGERS}" \
  > "$FLOG" 2>&1
FUZZ1_RC=$?
FUZZ1_RESULT=$(grep -o 'PASSED\|FAILED' "$FLOG" 2>/dev/null | tail -1 || echo "exit=${FUZZ1_RC}")
ok "Fuzz-5-no => ${FUZZ1_RESULT}"

stop_cluster

# ═══════════════════════════════════════════════════════════════════════════════
# FUZZ TEST 2: RF=5, crash leader + 1 follower mid-run
# ═══════════════════════════════════════════════════════════════════════════════
log "════════════════════════════════════════"
log "FUZZ 2: RF=5, crash leader + 1 follower"
log "════════════════════════════════════════"

start_cluster 5 2
source "${SCRIPT_DIR}/config.sh"
log "  Waiting 10s for Raft elections..."
sleep 10

FLOG2="/tmp/madkv-p3/fuzz/fuzz-5-yes.log"
log "  Starting fuzzer in background..."
cd "$MADKV"
"$FUZZER" \
  --num-clis 5 \
  --num-ops 1000 \
  --conflict \
  --client-just-args p3::client "${MANAGERS}" \
  > "$FLOG2" 2>&1 &
FUZZ_PID=$!

# Give the fuzzer 20s to get past setup and start issuing operations
log "  Waiting 20s for fuzzer to start..."
sleep 20

# Kill the leader of partition 0
log "  Injecting crash: finding p0 leader..."
find_leader 0
if [[ -n "$P_LEADER_HOST" ]]; then
  log "  Killing leader on ${P_LEADER_HOST}..."
  ssh_n "$P_LEADER_HOST" "pkill -9 kvserver 2>/dev/null; true"
  ok "  Leader killed"
else
  # Fallback: kill node2 (first p0 replica)
  warn "  Could not find leader, killing node2 (first p0 replica)..."
  ssh_n "${NODE_HOSTS[2]}" "pkill -9 kvserver 2>/dev/null; true"
fi

# Wait for new election
log "  Waiting 10s for new election..."
sleep 10

# Kill a follower (node3 or first available p0 node that still runs)
log "  Killing a p0 follower..."
for nidx in 3 4 5; do
  FHOST="${NODE_HOSTS[$nidx]}"
  if [[ "$FHOST" != "$P_LEADER_HOST" ]]; then
    ssh_n "$FHOST" "pkill -9 kvserver 2>/dev/null; true" && \
      ok "  Follower killed on ${FHOST}" && break
  fi
done

# Wait for fuzzer to finish (with timeout)
log "  Waiting for fuzzer to complete (max 300s)..."
WAIT=0
while kill -0 $FUZZ_PID 2>/dev/null && (( WAIT < 300 )); do
  sleep 5; WAIT=$((WAIT + 5))
done

if kill -0 $FUZZ_PID 2>/dev/null; then
  warn "  Fuzzer still running after timeout, killing..."
  kill $FUZZ_PID 2>/dev/null || true
  wait $FUZZ_PID 2>/dev/null || true
else
  wait $FUZZ_PID 2>/dev/null || true
fi

FUZZ2_RESULT=$(grep -o 'PASSED\|FAILED' "$FLOG2" 2>/dev/null | tail -1 || echo "timeout")
ok "Fuzz-5-yes => ${FUZZ2_RESULT}"

stop_cluster

# ═══════════════════════════════════════════════════════════════════════════════
# BENCH: 10 clients × workloads A-F × RF = 1, 3, 5
# ═══════════════════════════════════════════════════════════════════════════════
log "════════════════════════════════════════"
log "BENCH: 10 clients × workloads A-F × RF∈{1,3,5}"
log "════════════════════════════════════════"

for rf in 1 3 5; do
  log "  --- RF=${rf}: starting cluster ---"
  start_cluster "$rf" 1
  source "${SCRIPT_DIR}/config.sh"

  for wload in a b c d e f; do
    BLOG="/tmp/madkv-p3/bench/bench-10-${wload}-${rf}.log"
    log "  bench nclis=10 wload=${wload} rf=${rf} → ${BLOG}"
    cd "$MADKV"
    "$BENCHER" \
      --num-clis 10 \
      --workload "$wload" \
      --client-just-args p3::client "${MANAGERS}" \
      > "$BLOG" 2>&1
    TPUT=$(grep -i 'Throughput' "$BLOG" 2>/dev/null | awk '{print $(NF-1)}' | tail -1 || echo '?')
    ok "    rf=${rf} wload=${wload} nclis=10 => ${TPUT} ops/s"
  done

  stop_cluster
done

# ═══════════════════════════════════════════════════════════════════════════════
# BENCH: Scaling clients (1, 10, 20, 30) × workload A × RF = 1
# ═══════════════════════════════════════════════════════════════════════════════
log "════════════════════════════════════════"
log "BENCH: Scaling clients × wload-A × RF=1"
log "════════════════════════════════════════"

start_cluster 1 1
source "${SCRIPT_DIR}/config.sh"

for nclis in 1 10 20 30; do
  BLOG="/tmp/madkv-p3/bench/bench-${nclis}-a-1.log"
  log "  bench nclis=${nclis} wload=a rf=1 → ${BLOG}"
  cd "$MADKV"
  "$BENCHER" \
    --num-clis "$nclis" \
    --workload a \
    --client-just-args p3::client "${MANAGERS}" \
    > "$BLOG" 2>&1
  TPUT=$(grep -i 'Throughput' "$BLOG" 2>/dev/null | awk '{print $(NF-1)}' | tail -1 || echo '?')
  ok "    rf=1 wload=a nclis=${nclis} => ${TPUT} ops/s"
done

stop_cluster

# ═══════════════════════════════════════════════════════════════════════════════
# BENCH: Scaling clients (1, 10, 20, 30) × workload A × RF = 5
# ═══════════════════════════════════════════════════════════════════════════════
log "════════════════════════════════════════"
log "BENCH: Scaling clients × wload-A × RF=5"
log "════════════════════════════════════════"

start_cluster 5 1
source "${SCRIPT_DIR}/config.sh"

for nclis in 1 10 20 30; do
  BLOG="/tmp/madkv-p3/bench/bench-${nclis}-a-5.log"
  log "  bench nclis=${nclis} wload=a rf=5 → ${BLOG}"
  cd "$MADKV"
  "$BENCHER" \
    --num-clis "$nclis" \
    --workload a \
    --client-just-args p3::client "${MANAGERS}" \
    > "$BLOG" 2>&1
  TPUT=$(grep -i 'Throughput' "$BLOG" 2>/dev/null | awk '{print $(NF-1)}' | tail -1 || echo '?')
  ok "    rf=5 wload=a nclis=${nclis} => ${TPUT} ops/s"
done

stop_cluster

# ═══════════════════════════════════════════════════════════════════════════════
# Done
# ═══════════════════════════════════════════════════════════════════════════════
log ""
log "════════════════════════════════════════"
log "ALL TESTS DONE"
log "  fuzz logs : /tmp/madkv-p3/fuzz/"
log "  bench logs: /tmp/madkv-p3/bench/"
log "  master log: ${LOG_MASTER}"
log "════════════════════════════════════════"
ls -la /tmp/madkv-p3/fuzz/ 2>/dev/null
ls -la /tmp/madkv-p3/bench/ 2>/dev/null
