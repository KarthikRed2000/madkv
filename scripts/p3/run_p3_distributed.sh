#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run_p3_distributed.sh — Full P3 test suite on a CloudLab cluster.
#
# Run from node0.  Runs all required fuzz and YCSB bench scenarios, then
# generates the report template.
#
# Prerequisites:
#   • config.sh filled in with your node IPs / hostnames.
#   • madkv repo cloned and built on every node (kvstore/bin/* exist).
#   • Passwordless SSH from node0 to all other nodes.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CFG="${SCRIPT_DIR}/config.sh"
source "$CFG"

export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"

PLOG="/tmp/madkv-p3/run_all.log"
mkdir -p "$(dirname "$PLOG")"
: > "$PLOG"

log() { echo "[$(date '+%H:%M:%S')] $1" | tee -a "$PLOG"; }

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

start_cluster() {
  local rf="$1"
  local nparts="${2:-1}"
  # Override RF / NPARTS and restart
  bash "${SCRIPT_DIR}/start_all.sh" --clean --rf "$rf" --nparts "$nparts"
  sleep 3
}

stop_cluster() {
  bash "${SCRIPT_DIR}/stop_all.sh"
  sleep 1
}

ssh_cmd() {
  local host="$1"; shift
  ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$host" \
      "export PATH=\$HOME/.cargo/bin:\$HOME/.local/bin:\$PATH; $*"
}

# Build the project on every node in parallel
build_on_all_nodes() {
  log "Building madkv on all nodes in parallel..."
  for host in "${NODE_HOSTS[@]}"; do
    ssh_cmd "$host" "cd ${MADKV} && just p3::build" &
  done
  wait
  log "Build complete on all nodes."
}

run_fuzz() {
  local rf="$1"
  local crashing="${2:-no}"
  local nparts="${3:-1}"
  local logf="/tmp/madkv-p3/fuzz/fuzz-${rf}-${crashing}.log"
  mkdir -p /tmp/madkv-p3/fuzz

  log "FUZZ  rf=${rf}  crashing=${crashing}  nparts=${nparts}"
  start_cluster "$rf" "$nparts"
  source "$CFG"

  set +e
  cd "$MADKV"
  # Run fuzzer on the local orchestrator node (node0)
  cargo run -p runner -r --bin fuzzer -- \
      --num-clis 5 \
      --conflict \
      --client-just-args p3::client "${MANAGERS}" \
      > "$logf" 2>&1
  local rc=$?
  set -e

  stop_cluster
  local result
  result="$(grep -o 'PASSED\|FAILED' "$logf" 2>/dev/null | tail -1 || echo "exit=${rc}")"
  log "  => ${result}  (${logf})"
}

# Spawn kvclient processes on dedicated client nodes (nodes 12-21) in parallel,
# then collect their outputs back to node0.
run_bench() {
  local nclis="$1"
  local wload="$2"
  local rf="${3:-3}"
  local nparts="${4:-1}"
  local tag="bench-${nclis}-${wload}-rf${rf}"
  local logf="/tmp/madkv-p3/bench/${tag}.log"
  mkdir -p /tmp/madkv-p3/bench

  log "BENCH  nclis=${nclis}  workload=${wload}  rf=${rf}  nparts=${nparts}"
  start_cluster "$rf" "$nparts"
  source "$CFG"

  # Distribute clients: each dedicated client node runs one kvclient instance.
  # Up to 10 clients can run in parallel on CLIENT_HOSTS (nodes 12-21).
  local num_cli_nodes="${#CLIENT_HOSTS[@]}"   # 10
  local actual_nclis=$(( nclis < num_cli_nodes ? nclis : num_cli_nodes ))

  log "  Spawning ${actual_nclis} client(s) across dedicated nodes..."
  local pids=()
  for ((c=0; c<actual_nclis; c++)); do
    local remote_log="/tmp/madkv-p3/bench/${tag}-cli${c}.log"
    ssh_cmd "${CLIENT_HOSTS[$c]}" \
        "mkdir -p /tmp/madkv-p3/bench && \
         ${MADKV}/kvstore/bin/kvclient \
           --manager_addrs ${MANAGERS} \
           --workload ${wload} \
           > ${remote_log} 2>&1" &
    pids+=($!)
  done

  # Wait for all clients to finish
  local failed=0
  for pid in "${pids[@]}"; do
    wait "$pid" || failed=1
  done

  # Pull results back to node0
  > "$logf"
  for ((c=0; c<actual_nclis; c++)); do
    local remote_log="/tmp/madkv-p3/bench/${tag}-cli${c}.log"
    echo "=== client ${c} ===" >> "$logf"
    scp -o StrictHostKeyChecking=no \
        "${CLIENT_HOSTS[$c]}:${remote_log}" - >> "$logf" 2>/dev/null || true
  done

  stop_cluster

  local tp
  tp="$(grep -i 'throughput\|ops/sec' "$logf" 2>/dev/null | tail -1 | \
        awk '{print $(NF-1), $NF}' || echo "failed=${failed}")"
  log "  => tput=${tp}  (${logf})"
}

# ─────────────────────────────────────────────────────────────────────────────
# Test Suite
# ─────────────────────────────────────────────────────────────────────────────
log "════════════════════════════════════════════════"
log "  P3 Distributed Test Suite"
log "  Nodes: ${#NODE_HOSTS[@]}  default RF: ${RF}  NPARTS: ${NPARTS}"
log "════════════════════════════════════════════════"

cd "$MADKV"
just p3::build   # build locally (node0) for the fuzzer / bencher runner
just utils::build
just utils::ycsb
build_on_all_nodes  # build on every other node so servers + clients are ready

log ""
log "═══ FUZZ TESTS ═════════════════════════════════"

# Required: rf=5, no crash
run_fuzz 5 no 1

# Required: rf=5, crash 2 servers (one of them leader)
# NOTE: You must manually kill 2 servers during the fuzzer run.
#       Start the cluster, wait for fuzzer to start, then:
#         ssh <leader_node> "pkill -9 kvserver"
#         ssh <follower_node> "pkill -9 kvserver"
log ""
log "Starting rf=5 cluster for CRASHING fuzz test."
log "After fuzzer starts, crash 2 servers (at least 1 leader) then wait for completion."
run_fuzz 5 yes 1

log ""
log "═══ YCSB BENCHMARKS ════════════════════════════"

log "--- 10 clients × workloads A-F × RF=1,3,5 ---"
for rf_val in 1 3 5; do
  for wload in a b c d e f; do
    run_bench 10 "$wload" "$rf_val" 1
  done
done

log "--- Workload A, scaling clients, RF=1 ---"
for nclis in 1 5 10 20 30; do
  run_bench "$nclis" a 1 1
done

log "--- Workload A, scaling clients, RF=5 ---"
for nclis in 1 5 10 20 30; do
  run_bench "$nclis" a 5 1
done

log ""
log "═══ REPORT ═════════════════════════════════════"
cd "$MADKV"
mkdir -p report
python3 sumgen/proj3.py 2>&1 | tee -a "$PLOG" || log "Report script returned non-zero (check sumgen output)"

log ""
log "ALL DONE"
log "  fuzz results : /tmp/madkv-p3/fuzz/"
log "  bench results: /tmp/madkv-p3/bench/"
log "  full log     : ${PLOG}"
