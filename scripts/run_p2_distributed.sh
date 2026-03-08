#!/usr/bin/env bash
# Run P2 tests with each partition on a different physical machine.
# Usage: Run this script from node0 (client node).
# Prereqs: passwordless SSH from node0 to nodes 1-6; madkv at ~/madkv on all nodes.
#
# Configure your CloudLab experiment's private IPs for inter-node communication.
# Check your experiment topology for the actual assignment.
set -euo pipefail

# SSH targets (node index -> user@host)
NODE_HOSTS=(
  "jann2000@ms0642.utah.cloudlab.us"  # node0 - client
  "jann2000@ms0614.utah.cloudlab.us"  # node1 - manager only
  "jann2000@ms0619.utah.cloudlab.us"  # node2 - server 0
  "jann2000@ms0604.utah.cloudlab.us"  # node3 - server 1
  "jann2000@ms0603.utah.cloudlab.us"  # node4 - server 2
  "jann2000@ms0640.utah.cloudlab.us"  # node5 - server 3
  "jann2000@ms0636.utah.cloudlab.us"  # node6 - server 4
)

# Private IPs for gRPC (from ifconfig on each node; enp1s0d1 interface)
# Verified: node0=10.10.1.1, node1=10.10.1.2; nodes 2-6 assumed sequential
NODE_IPS=(
  "10.10.1.1"   # node0 (ms0642) - verified
  "10.10.1.2"   # node1 (ms0614) - verified
  "10.10.1.3"   # node2 (ms0619) - run ifconfig to confirm
  "10.10.1.4"   # node3 (ms0604)
  "10.10.1.5"   # node4 (ms0603)
  "10.10.1.6"   # node5 (ms0640)
  "10.10.1.7"   # node6 (ms0636)
)

MANAGER="${NODE_IPS[1]}:3666"
MADKV="$HOME/madkv"
BACKER="/tmp/madkv-p2-backer/backer"
PLOG="/tmp/madkv-p2-progress.log"

export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"

log() { echo "[$(date '+%H:%M:%S')] $1" | tee -a "$PLOG"; }

# Run command on remote node by index (1-6 are server nodes)
remote() {
  local node_idx="$1"
  shift
  ssh -o StrictHostKeyChecking=no "${NODE_HOSTS[$node_idx]}" \
    "export PATH=\$HOME/.cargo/bin:\$PATH; $*"
}

# Kill processes on a remote node
remote_kill() {
  remote "$1" "cd $MADKV && just p2::kill" 2>/dev/null || true
}

get_servers() {
  local np="$1"
  local list=""
  local i
  for ((i = 0; i < np; i++)); do
    [[ -n "$list" ]] && list+=","
    list+="${NODE_IPS[$((i+2))]}:3777"
  done
  echo "$list"
}

kill_cluster() {
  just p2::kill 2>/dev/null || true
  for i in 1 2 3 4 5 6; do
    remote_kill "$i" &
  done
  wait
  sleep 1
}

start_cluster() {
  local np="$1"
  local svrs
  svrs="$(get_servers "$np")"
  kill_cluster
  log "Starting ${np}-partition cluster (manager on node1, partitions on nodes 2-$((np+1)))"

  # Clean backer dir on node1 (manager log) and on server nodes 2..np+1
  remote 1 "rm -rf /tmp/madkv-p2-backer && mkdir -p /tmp/madkv-p2-backer"
  for ((i = 2; i <= np + 1; i++)); do
    remote "$i" "rm -rf /tmp/madkv-p2-backer && mkdir -p /tmp/madkv-p2-backer" &
  done
  wait

  # Start manager on node1 only
  remote 1 "nohup $MADKV/kvstore/bin/kvmanager 0.0.0.0:3666 '$svrs' >/tmp/madkv-p2-backer/mgr.log 2>&1 & disown"
  sleep 1

  # Start one server per node (server i on node i+2)
  for ((i = 0; i < np; i++)); do
    local node=$((i + 2))
    remote "$node" "nohup $MADKV/kvstore/bin/kvserver $i $MANAGER 3777 ${BACKER}.s$i >/tmp/madkv-p2-backer/s${i}.log 2>&1 & disown" &
  done
  wait
  sleep 5
  log "Cluster ready"
}

run_fuzz() {
  local np="$1" cr="$2"
  log "FUZZ ${np} parts crashing=${cr}"
  start_cluster "$np"
  cd "$MADKV"
  mkdir -p /tmp/madkv-p2/fuzz
  local logf="/tmp/madkv-p2/fuzz/fuzz-${np}-${cr}.log"
  set +e
  cargo run -p runner -r --bin fuzzer -- \
    --num-clis 5 --conflict \
    --client-just-args p2::client "$MANAGER" \
    >"$logf" 2>&1
  local rc=$?
  set -e
  kill_cluster
  local res
  res="$(grep -o 'PASSED\|FAILED' "$logf" 2>/dev/null || echo "exit=$rc")"
  log "FUZZ ${np}-${cr} => $res"
}

run_bench() {
  local nc="$1" wl="$2" np="$3"
  log "BENCH ${nc}cli ${wl} ${np}parts"
  start_cluster "$np"
  cd "$MADKV"
  mkdir -p /tmp/madkv-p2/bench
  local logf="/tmp/madkv-p2/bench/bench-${nc}-${wl}-${np}.log"
  set +e
  cargo run -p runner -r --bin bencher -- \
    --num-clis "$nc" --workload "$wl" \
    --client-just-args p2::client "$MANAGER" \
    >"$logf" 2>&1
  local rc=$?
  set -e
  kill_cluster
  local tp
  tp="$(grep 'Throughput' "$logf" 2>/dev/null | tail -1 | awk '{print $(NF-1)}' || echo "exit=$rc")"
  log "BENCH ${nc}-${wl}-${np} => tput=$tp"
}

: > "$PLOG"
log "P2 Distributed Test Suite - 7 nodes, 1 partition per machine"

rm -rf /tmp/madkv-p2/fuzz /tmp/madkv-p2/bench
mkdir -p /tmp/madkv-p2/fuzz /tmp/madkv-p2/bench

log "=== FUZZ TESTS ==="
run_fuzz 3 no
run_fuzz 3 yes
run_fuzz 5 yes

log "=== BENCH: 10cli all workloads ==="
for p in 1 3 5; do
  for w in a b c d e f; do
    run_bench 10 "$w" "$p"
  done
done

log "=== BENCH: scaling clients ==="
for p in 1 5; do
  for c in 1 20 30; do
    run_bench "$c" a "$p"
  done
done

log "=== REPORT ==="
cd "$MADKV"
mkdir -p report/plots-p2
echo "" | python3 sumgen/proj2.py -i /tmp/madkv-p2 -o report 2>&1 || {
  log "Report issue, trying script..."
  ./scripts/generate_p2_report.sh /tmp/madkv-p2 report 2>&1 || true
}

log "ALL DONE - fuzz=$(ls /tmp/madkv-p2/fuzz/*.log 2>/dev/null | wc -l) bench=$(ls /tmp/madkv-p2/bench/*.log 2>/dev/null | wc -l)"
