#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# cloudlab_test.sh — Non-interactive P3 correctness test suite for CloudLab
#
# Runs all 4 demo scenarios automatically (no pauses).
# Run from node0: bash scripts/p3/cloudlab_test.sh
# ─────────────────────────────────────────────────────────────────────────────
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

BIN="${MADKV}/kvstore/bin"
PASS=0; FAIL=0

# ── Helpers ───────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; RED='\033[0;31m'; CYAN='\033[0;36m'
BOLD='\033[1m'; YELLOW='\033[0;33m'; RESET='\033[0m'

log()  { echo -e "${CYAN}[$(date +%H:%M:%S)] $*${RESET}"; }
pass() { echo -e "${GREEN}  ✓ PASS: $*${RESET}"; (( PASS++ )) || true; }
fail() { echo -e "${RED}  ✗ FAIL: $*${RESET}"; (( FAIL++ )) || true; }
sep()  { echo -e "\n${BOLD}${CYAN}──────────────────────────────────────────${RESET}"; }

ssh_node() {
  local host="$1"; shift
  ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$host" \
      "export PATH=\$HOME/.cargo/bin:\$HOME/.local/bin:\$PATH; $*" 2>/dev/null
}

kvcmd() {
  ( printf '%s\n' "$@"; printf 'STOP\n' ) \
    | timeout 15 "${BIN}/kvclient" --manager_addrs "${MANAGERS}" 2>/dev/null \
    | grep -v '^STOP$'
}

assert_contains() {
  local desc="$1" needle="$2" haystack="$3"
  if echo "$haystack" | grep -qF "$needle"; then
    pass "$desc"
  else
    fail "$desc — expected '$needle' in: $haystack"
  fi
}

assert_eq() {
  local desc="$1" expected="$2" actual="$3"
  if [[ "$expected" == "$actual" ]]; then
    pass "$desc"
  else
    fail "$desc — expected='$expected'  actual='$actual'"
  fi
}

restart_cluster() {
  log "Restarting cluster (clean, RF=${RF}, NPARTS=${NPARTS})..."
  bash "${SCRIPT_DIR}/start_all.sh" --clean --rf "${RF}" --nparts "${NPARTS}" 2>&1 \
    | grep -E "READY|ERROR" || true
  sleep 3
}

kill_node() {
  local host="$1"
  ssh_node "$host" "pkill -9 kvserver 2>/dev/null; true"
}

# Find highest-term leader in a partition; sets P_LEADER_NODE, P_LEADER_IDX
find_leader() {
  local part="$1"   # 0 or 1
  local best_term=-1
  P_LEADER_NODE=""
  P_LEADER_IDX=-1
  for ((r=0; r<RF; r++)); do
    local node_idx=$(( part * RF + r + 2 ))
    local host="${NODE_HOSTS[$node_idx]}"
    local tag="s${part}.${r}"
    local logf="${LOG_DIR}/server-${tag}.log"
    local term
    term=$(ssh_node "$host" \
      "grep 'became LEADER' '$logf' 2>/dev/null | grep -oE 'term=[0-9]+' | grep -oE '[0-9]+' | sort -n | tail -1" \
      2>/dev/null || echo "")
    [[ -z "$term" || "$term" == "0" ]] && continue
    if (( term > best_term )); then
      best_term=$term
      P_LEADER_IDX=$node_idx
      P_LEADER_NODE="$host"
    fi
  done
}

# ── Preamble ──────────────────────────────────────────────────────────────────
echo -e "\n${BOLD}${CYAN}╔══════════════════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}${CYAN}║       CloudLab P3 Correctness Test Suite  (NPARTS=2, RF=5)    ║${RESET}"
echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════════════════╝${RESET}"
echo -e "  MANAGERS : ${MANAGERS}"
echo -e "  NPARTS   : ${NPARTS}   RF : ${RF}"
echo ""

# ── TEST 1: Basic operations ──────────────────────────────────────────────────
sep
log "TEST 1: Basic PUT / GET / SWAP / DELETE / SCAN"
restart_cluster

out=$(kvcmd \
  "PUT t1_apple fruit" \
  "PUT t1_berry fruit" \
  "PUT t1_cherry fruit" \
  "GET t1_apple" \
  "GET t1_berry" \
  "SWAP t1_apple pie" \
  "GET t1_apple" \
  "DELETE t1_berry" \
  "GET t1_berry")
assert_contains "GET after PUT"    "GET t1_apple fruit"   "$out"
assert_contains "GET after PUT b2" "GET t1_berry fruit"   "$out"
assert_contains "SWAP returns old" "SWAP t1_apple fruit"  "$out"
assert_contains "GET after SWAP"   "GET t1_apple pie"     "$out"
assert_contains "DELETE returns found" "DELETE t1_berry found" "$out"
assert_contains "GET after DELETE" "GET t1_berry null"    "$out"

out2=$(kvcmd "SCAN t1_apple t1_cherry")
assert_contains "SCAN range has t1_apple"  "t1_apple pie"  "$out2"
assert_contains "SCAN range has t1_cherry" "t1_cherry fruit" "$out2"

# ── TEST 2: Multi-partition routing ──────────────────────────────────────────
sep
log "TEST 2: Multi-partition routing (keys spread across p0 and p1)"

puts=()
for i in $(seq 1 20); do puts+=("PUT mp_key${i} v${i}"); done
kvcmd "${puts[@]}" > /dev/null

gets=()
for i in $(seq 1 20); do gets+=("GET mp_key${i}"); done
out=$(kvcmd "${gets[@]}")

all_ok=1
for i in $(seq 1 20); do
  echo "$out" | grep -qF "GET mp_key${i} v${i}" || { all_ok=0; break; }
done
[[ $all_ok -eq 1 ]] && pass "All 20 keys correct across both partitions" \
                     || fail "Some keys missing/wrong: $out"

# ── TEST 3: Follower failures (Demo 1) ───────────────────────────────────────
sep
log "TEST 3: DEMO 1 — 2 follower failures in each partition (RF=5, f=2)"
restart_cluster

# Write baseline
kvcmd "PUT demo1_x hello" "PUT demo1_y world" > /dev/null

# Kill 2 followers per partition (not the leader)
# Partition 0: first find leader, then kill non-leaders
find_leader 0
log "  p0 leader: ${P_LEADER_NODE} (node_idx=${P_LEADER_IDX})"

killed_p0=0
for ((node_idx=2; node_idx<=6 && killed_p0<2; node_idx++)); do
  host="${NODE_HOSTS[$node_idx]}"
  [[ "$host" == "$P_LEADER_NODE" ]] && continue
  kill_node "$host"
  log "  killed follower: $host"
  (( killed_p0++ )) || true
done

find_leader 1
log "  p1 leader: ${P_LEADER_NODE} (node_idx=${P_LEADER_IDX})"
killed_p1=0
for ((node_idx=7; node_idx<=11 && killed_p1<2; node_idx++)); do
  host="${NODE_HOSTS[$node_idx]}"
  [[ "$host" == "$P_LEADER_NODE" ]] && continue
  kill_node "$host"
  log "  killed follower: $host"
  (( killed_p1++ )) || true
done

sleep 2

out=$(kvcmd \
  "GET demo1_x" \
  "GET demo1_y" \
  "PUT demo1_z after_crash" \
  "GET demo1_z")
assert_contains "Read p0 key with 2 followers down" "GET demo1_x hello" "$out"
assert_contains "Read p1 key with 2 followers down" "GET demo1_y world" "$out"
assert_contains "Write with 2 followers down"       "GET demo1_z after_crash" "$out"

# ── TEST 4: Leader failure + election (Demo 2) ────────────────────────────────
sep
log "TEST 4: DEMO 2 — Leader failure + Raft election + transparent client recovery"
restart_cluster

kvcmd "PUT d2_before data_pre_crash" > /dev/null
sleep 1

find_leader 0
if [[ -n "$P_LEADER_NODE" ]]; then
  OLD_LEADER="$P_LEADER_NODE"
  log "  Killing p0 leader: $OLD_LEADER"
  kill_node "$OLD_LEADER"
else
  # default: kill node2 (first server)
  OLD_LEADER="${NODE_HOSTS[2]}"
  kill_node "$OLD_LEADER"
  log "  Killed default node: $OLD_LEADER"
fi

log "  Waiting 6s for election..."
sleep 6

# Client should find new leader transparently
out=""
for attempt in 1 2 3 4 5; do
  out=$(kvcmd "GET d2_before" "PUT d2_after post_election_data" "GET d2_after" 2>/dev/null || echo "")
  echo "$out" | grep -qF "GET d2_before data_pre_crash" && break
  log "  (attempt $attempt — retrying)"
  sleep 2
done

assert_contains "Pre-crash data survives leader fail" "GET d2_before data_pre_crash" "$out"
assert_contains "New write after leader fail"         "GET d2_after post_election_data" "$out"

# ── TEST 5: Quorum loss (Demo 4) ──────────────────────────────────────────────
sep
log "TEST 5: DEMO 4 — >f failures → partition stalls; other partition works"
restart_cluster

# Find a key in each partition by writing many keys and checking server logs
log "  Writing 40 keys and probing partition assignment..."
puts=()
for i in $(seq 1 40); do puts+=("PUT probe_${i} v${i}"); done
kvcmd "${puts[@]}" > /dev/null
sleep 1

# Find partition 0 leader and check which keys it applied
find_leader 0
P0_LEADER_NODE="$P_LEADER_NODE"
p0_tag="s0.$((P_LEADER_IDX - 2))"
logf="${LOG_DIR}/server-${p0_tag}.log"

P0_KEY=""
P1_KEY=""
for i in $(seq 1 40); do
  key="probe_${i}"
  applied=$(ssh_node "$P0_LEADER_NODE" "grep -c 'Apply.*${key}\|${key}.*Apply' '$logf' 2>/dev/null || echo 0" 2>/dev/null || echo 0)
  if [[ "${applied:-0}" -gt 0 && -z "$P0_KEY" ]]; then
    P0_KEY="$key"
  elif [[ "${applied:-0}" -eq 0 && -z "$P1_KEY" ]]; then
    P1_KEY="$key"
  fi
  [[ -n "$P0_KEY" && -n "$P1_KEY" ]] && break
done

# Fallback: just kill all 5 partition 0 servers; try many keys; some will stall
if [[ -z "$P0_KEY" ]]; then
  log "  Could not determine partition 0 key from logs, killing all p0 nodes..."
  for node_idx in 2 3 4 5 6; do kill_node "${NODE_HOSTS[$node_idx]}"; done
  sleep 2
  stalled=0
  for i in $(seq 1 20); do
    key="probe_${i}"
    result=$(timeout 4 bash -c \
      "(printf 'GET ${key}\nSTOP\n') | '${BIN}/kvclient' --manager_addrs '${MANAGERS}'" \
      2>/dev/null || echo "TIMEOUT")
    if echo "$result" | grep -qiE "TIMEOUT|^$"; then
      (( stalled++ )) || true
      P0_KEY="$key"
      break
    fi
  done
  [[ $stalled -gt 0 ]] && pass "Some p0 keys stalled as expected (no quorum)" \
                        || fail "No keys stalled after killing all p0 replicas"
else
  log "  P0 key: $P0_KEY  P1 key: ${P1_KEY:-unknown}"
  log "  Killing 3 partition-0 replicas (> f=2)..."
  for node_idx in 2 3 4; do kill_node "${NODE_HOSTS[$node_idx]}"; done
  sleep 2

  # P1 should still work
  out_p1=$(kvcmd "PUT p1_sentinel p1_alive" "GET p1_sentinel")
  assert_contains "p1 writable while p0 quorum lost" "GET p1_sentinel p1_alive" "$out_p1"

  # P0 key should stall (commit requires Raft majority)
  log "  Testing p0 stall with PUT to p0 key (expect timeout ~8s)..."
  p0_out=$(timeout 8 bash -c \
    "(printf 'PUT ${P0_KEY} stall_value\nSTOP\n') | '${BIN}/kvclient' --manager_addrs '${MANAGERS}'" \
    2>/dev/null || echo "TIMEOUT_OR_ERROR")
  if echo "$p0_out" | grep -qiE "TIMEOUT_OR_ERROR|^$"; then
    pass "p0 PUT stalled (no quorum); p1 unaffected"
  else
    fail "p0 PUT should have stalled but returned: $p0_out"
  fi
fi

# ── TEST 6: Manager crash + restart ───────────────────────────────────────────
sep
log "TEST 6: DEMO 3 — Manager crash + restart + persistent state"
restart_cluster

kvcmd "PUT mgr_key1 before_crash" "PUT mgr_key2 also_before" > /dev/null

log "  Killing manager (node1)..."
ssh_node "${NODE_HOSTS[1]}" "pkill -9 kvmanager 2>/dev/null; true"
sleep 2

log "  Restarting manager..."
ssh_node "${NODE_HOSTS[1]}" "nohup bash ${MADKV}/scripts/p3/start_manager.sh ${MADKV}/scripts/p3/config.sh >/tmp/madkv-p3/logs/mgr_restart.log 2>&1 &"
sleep 6  # give it more time to bind and accept connections

out=""
for attempt in 1 2 3 4 5; do
  out=$(kvcmd "GET mgr_key1" "GET mgr_key2" 2>/dev/null || echo "")
  echo "$out" | grep -qF "mgr_key1 before_crash" && break
  log "  (manager restart retry $attempt)"
  sleep 3
done
assert_contains "Data readable after manager restart" "GET mgr_key1 before_crash" "$out"
assert_contains "Data readable after manager restart 2" "GET mgr_key2 also_before" "$out"

# ── TEST 7: Full cluster restart (persistence) ────────────────────────────────
sep
log "TEST 7: Full cluster restart — data persisted (no --clean)"
restart_cluster

kvcmd "PUT persist_key1 val_one" "PUT persist_key2 val_two" > /dev/null
log "  Data written. Stopping all servers..."
bash "${SCRIPT_DIR}/stop_all.sh" 2>/dev/null | tail -2

log "  Restarting cluster (no --clean, keep data)..."
bash "${SCRIPT_DIR}/start_all.sh" --rf "${RF}" --nparts "${NPARTS}" 2>&1 \
  | grep -E "READY|ERROR" || true
sleep 4

out=""
for attempt in 1 2 3; do
  out=$(kvcmd "GET persist_key1" "GET persist_key2" 2>/dev/null || echo "")
  echo "$out" | grep -qF "persist_key1 val_one" && break
  sleep 2
done
assert_contains "Key 1 persisted across restart" "GET persist_key1 val_one" "$out"
assert_contains "Key 2 persisted across restart" "GET persist_key2 val_two" "$out"

# ── TEST 8: Concurrent clients ────────────────────────────────────────────────
sep
log "TEST 8: Concurrent clients from multiple nodes"
restart_cluster

WRITES_OK=0
pids=()
# Use client nodes 12-16; if unreachable fall back to node0 (orchestrator)
CLIENT_NODES=(node12 node13 node14 node15 node16)
USED_CLIENTS=()
for cn in "${CLIENT_NODES[@]}"; do
  if ssh_node "$cn" "true" 2>/dev/null; then
    USED_CLIENTS+=("$cn")
  fi
done
[[ ${#USED_CLIENTS[@]} -eq 0 ]] && USED_CLIENTS=(node0)

log "  Using ${#USED_CLIENTS[@]} client node(s): ${USED_CLIENTS[*]}"
for cn in "${USED_CLIENTS[@]}"; do
  ssh_node "$cn" \
    "( printf 'PUT cc_${cn}_k1 val1\nPUT cc_${cn}_k2 val2\nSTOP\n' ) | \
     ${MADKV}/kvstore/bin/kvclient --manager_addrs ${MANAGERS} 2>/dev/null" &
  pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid" && (( WRITES_OK++ )) || true; done
log "  $WRITES_OK / ${#pids[@]} concurrent write batches completed"

sleep 1
reads_ok=0
for cn in "${USED_CLIENTS[@]}"; do
  out=$(kvcmd "GET cc_${cn}_k1" "GET cc_${cn}_k2")
  echo "$out" | grep -qF "cc_${cn}_k1 val1" && \
  echo "$out" | grep -qF "cc_${cn}_k2 val2" && (( reads_ok++ )) || true
done
total_clients=${#USED_CLIENTS[@]}
(( reads_ok == total_clients )) && \
  pass "All $total_clients concurrent clients' writes visible (reads_ok=$reads_ok)" || \
  fail "Only $reads_ok/${total_clients} clients' writes visible"

# ── SUMMARY ───────────────────────────────────────────────────────────────────
sep
echo ""
echo -e "${BOLD}╔══════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}║              RESULTS SUMMARY              ║${RESET}"
echo -e "${BOLD}╠══════════════════════════════════════════╣${RESET}"
echo -e "${BOLD}║  PASSED : $(printf '%-3s' $PASS)                           ║${RESET}"
echo -e "${BOLD}║  FAILED : $(printf '%-3s' $FAIL)                           ║${RESET}"
echo -e "${BOLD}╚══════════════════════════════════════════╝${RESET}"
echo ""
[[ $FAIL -eq 0 ]] && echo -e "${GREEN}${BOLD}All tests PASSED!${RESET}" || \
                     echo -e "${RED}${BOLD}${FAIL} test(s) FAILED.${RESET}"
exit $FAIL
