#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# custom_test.sh — P3 correctness test suite
#
# Tests:
#   1.  All 5 KV operations (PUT/GET/SWAP/DELETE/SCAN)  [RF=1, 1 partition]
#   2.  Multi-partition key routing                      [RF=1, 2 partitions]
#   3.  Leader election after leader crash               [RF=3, 1 partition]
#   4.  Follower crash + Raft log catch-up               [RF=3, 1 partition]
#   5.  Minority cannot make progress (no quorum)        [RF=3, 1 partition]
#   6.  Full cluster restart — persistence               [RF=3, 1 partition]
#   7.  Concurrent clients — no lost writes              [RF=3, 1 partition]
#   8.  RF=5 — tolerate 2 simultaneous crashes           [RF=5, 1 partition]
#   9.  SWAP atomicity — old value always correct        [RF=3, 1 partition]
#  10.  SCAN across multiple partitions                  [RF=1, 2 partitions]
#
# Usage:
#   bash scripts/p3/custom_test.sh            # run all tests
#   bash scripts/p3/custom_test.sh 3 5 7      # run only listed tests
#
# Prerequisites: kvstore/bin/{kvserver,kvclient,kvmanager} must be built.
# ─────────────────────────────────────────────────────────────────────────────
set -o pipefail

BIN="$(cd "$(dirname "$0")/../.." && pwd)/kvstore/bin"
# Use TESTDIR (not TMPDIR — that's a special macOS env var that would conflict)
TESTDIR="/tmp/madkv-p3-test"
LOGDIR="$TESTDIR/logs"
PASS=0; FAIL=0; SKIP=0
SELECTED=(); for arg in "$@"; do SELECTED+=("$arg"); done

# ── Colour helpers ────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[0;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

log()  { echo -e "${CYAN}[$(date '+%H:%M:%S')]${RESET} $*"; }
pass() { echo -e "${GREEN}${BOLD}  PASS${RESET} $*"; ((PASS++)); }
fail() { echo -e "${RED}${BOLD}  FAIL${RESET} $*"; ((FAIL++)); }
skip() { echo -e "${YELLOW}  SKIP${RESET} $*"; ((SKIP++)); }
hdr()  {
  echo -e "\n${BOLD}${CYAN}══════════════════════════════════════════════════════${RESET}"
  echo -e "${BOLD}  TEST $1: $2${RESET}"
  echo -e "${BOLD}${CYAN}══════════════════════════════════════════════════════${RESET}"
}

should_run() {
  [[ ${#SELECTED[@]} -eq 0 ]] && return 0
  for t in "${SELECTED[@]}"; do [[ "$t" == "$1" ]] && return 0; done
  return 1
}

# ── Port assignments ──────────────────────────────────────────────────────────
MAN_PORT=15666
API_BASE=15777   # server $i listens on API_BASE+i
P2P_BASE=15707   # server $i listens on P2P_BASE+i for Raft

MANAGERS="127.0.0.1:$MAN_PORT"
CLUSTER_RF=1; CLUSTER_NPARTS=1

# ── Hard kill everything on our ports ────────────────────────────────────────
kill_all() {
  pkill -9 kvmanager 2>/dev/null || true
  pkill -9 kvserver  2>/dev/null || true
  pkill -9 kvclient  2>/dev/null || true
  sleep 1   # give the OS time to release ports
}

# start_cluster <nparts> <rf>
#   Kills any running cluster, wipes state, starts a fresh one, waits until ready.
start_cluster() {
  local nparts="$1" rf="$2"
  CLUSTER_RF=$rf; CLUSTER_NPARTS=$nparts
  kill_all
  rm -rf "$TESTDIR"; mkdir -p "$LOGDIR" "$TESTDIR/m0"

  # Build --server_addrs list (all replicas across all partitions)
  local total=$((nparts * rf))
  local server_addrs=""
  for ((i=0; i<total; i++)); do
    [[ -n "$server_addrs" ]] && server_addrs+=","
    server_addrs+="127.0.0.1:$((API_BASE+i))"
    mkdir -p "$TESTDIR/s$i"
  done

  # Start manager
  "$BIN/kvmanager" \
      --replica_id 0 --man_port $MAN_PORT --p2p_port $((MAN_PORT-60)) \
      --peer_addrs none --server_rf "$rf" \
      --server_addrs "$server_addrs" \
      --backer_path "$TESTDIR/m0" \
      > "$LOGDIR/manager.log" 2>&1 &

  # Start each server replica — peers are ONLY within the same partition
  for ((i=0; i<total; i++)); do
    local part=$((i / rf)) rep=$((i % rf))
    local peers=""
    for ((r=0; r<rf; r++)); do
      [[ $r -eq $rep ]] && continue
      local pidx=$((part * rf + r))
      [[ -n "$peers" ]] && peers+=","
      peers+="127.0.0.1:$((P2P_BASE+pidx))"
    done
    "$BIN/kvserver" \
        --partition_id "$part" --replica_id "$rep" \
        --manager_addrs "$MANAGERS" \
        --api_port $((API_BASE+i)) --p2p_port $((P2P_BASE+i)) \
        --peer_addrs "${peers:-none}" \
        --backer_path "$TESTDIR/s$i" \
        > "$LOGDIR/server-$i.log" 2>&1 &
  done

  # Wait until every partition has a leader AND manager reports ready
  local waited=0
  while true; do
    local leaders
    leaders=$(grep -l "became LEADER" "$LOGDIR"/server-*.log 2>/dev/null | wc -l | tr -d ' ')
    local mgr_ready
    mgr_ready=$(grep -c "cluster ready" "$LOGDIR/manager.log" 2>/dev/null || true)
    # Each partition needs at least one leader entry; mgr must have logged "ready"
    if [[ $leaders -ge $nparts ]] && \
       grep -q "cluster ready state: ready" "$LOGDIR/manager.log" 2>/dev/null; then
      break
    fi
    # Fallback: just wait for one leader per partition when mgr never logs "ready"
    if [[ $leaders -ge $nparts ]] && [[ $waited -ge 4 ]]; then
      break
    fi
    sleep 0.5; waited=$((waited+1))
    if [[ $waited -gt 24 ]]; then
      log "WARN: cluster may not be fully ready after 12s (leaders=$leaders nparts=$nparts)" >&2
      break
    fi
  done
  sleep 0.5   # extra settle
}

# Restart a cluster that was previously kill_all'd, preserving backer state.
restart_cluster() {
  local nparts=$CLUSTER_NPARTS rf=$CLUSTER_RF
  local total=$((nparts * rf))
  local server_addrs=""
  for ((i=0; i<total; i++)); do
    [[ -n "$server_addrs" ]] && server_addrs+=","
    server_addrs+="127.0.0.1:$((API_BASE+i))"
  done

  "$BIN/kvmanager" \
      --replica_id 0 --man_port $MAN_PORT --p2p_port $((MAN_PORT-60)) \
      --peer_addrs none --server_rf "$rf" \
      --server_addrs "$server_addrs" \
      --backer_path "$TESTDIR/m0" \
      >> "$LOGDIR/manager.log" 2>&1 &

  for ((i=0; i<total; i++)); do
    local part=$((i / rf)) rep=$((i % rf))
    local peers=""
    for ((r=0; r<rf; r++)); do
      [[ $r -eq $rep ]] && continue
      local pidx=$((part * rf + r))
      [[ -n "$peers" ]] && peers+=","
      peers+="127.0.0.1:$((P2P_BASE+pidx))"
    done
    "$BIN/kvserver" \
        --partition_id "$part" --replica_id "$rep" \
        --manager_addrs "$MANAGERS" \
        --api_port $((API_BASE+i)) --p2p_port $((P2P_BASE+i)) \
        --peer_addrs "${peers:-none}" \
        --backer_path "$TESTDIR/s$i" \
        >> "$LOGDIR/server-$i.log" 2>&1 &
  done

  # Wait for a leader per partition
  local waited=0
  while true; do
    local leaders
    leaders=$(grep -l "became LEADER" "$LOGDIR"/server-*.log 2>/dev/null | wc -l | tr -d ' ')
    [[ $leaders -ge $nparts ]] && break
    sleep 0.5; waited=$((waited+1)); [[ $waited -gt 24 ]] && break
  done
  sleep 1
}

# Restart a single server from its persisted backer
restart_server() {
  local i="$1" rf=$CLUSTER_RF nparts=$CLUSTER_NPARTS
  local part=$((i / rf)) rep=$((i % rf))
  local peers=""
  for ((r=0; r<rf; r++)); do
    [[ $r -eq $rep ]] && continue
    local pidx=$((part * rf + r))
    [[ -n "$peers" ]] && peers+=","
    peers+="127.0.0.1:$((P2P_BASE+pidx))"
  done
  "$BIN/kvserver" \
      --partition_id "$part" --replica_id "$rep" \
      --manager_addrs "$MANAGERS" \
      --api_port $((API_BASE+i)) --p2p_port $((P2P_BASE+i)) \
      --peer_addrs "${peers:-none}" \
      --backer_path "$TESTDIR/s$i" \
      >> "$LOGDIR/server-$i.log" 2>&1 &
}

# Kill one server by its global index (0-based)
kill_server() {
  local port=$((API_BASE+$1))
  # macOS: pkill -f matches against full command line
  pkill -9 -f -- "--api_port $port " 2>/dev/null || \
  pkill -9 -f -- "api_port $port"   2>/dev/null || true
  sleep 0.3
}

# Send KV commands to the cluster via kvclient.
# Each positional arg is one command line; STOP is appended automatically.
kvcmd() {
  ( printf '%s\n' "$@"; printf 'STOP\n' ) \
    | "$BIN/kvclient" --manager_addrs "$MANAGERS" 2>/dev/null \
    | grep -v '^STOP$'
}

# assert_eq <label> <expected> <actual>
assert_eq() {
  local label="$1" expected="$2" actual="$3"
  if [[ "$actual" == "$expected" ]]; then
    pass "$label"
  else
    fail "$label"
    echo "    expected: $(echo "$expected" | head -3)"
    echo "    got:      $(echo "$actual"   | head -3)"
  fi
}

# leader_idx <partition>  — global server index of current leader for that partition
leader_idx() {
  local p="$1" rf=$CLUSTER_RF best_term=-1 best_idx=-1
  for ((r=0; r<rf; r++)); do
    local idx=$((p*rf+r))
    local lf="$LOGDIR/server-$idx.log"
    [[ -f "$lf" ]] || continue
    # Extract the highest term from all "became LEADER" entries in this log
    local term
    term=$(grep "became LEADER" "$lf" 2>/dev/null \
           | grep -oE 'term[= ]+[0-9]+' | grep -oE '[0-9]+' | tail -1)
    [[ -z "$term" ]] && continue
    if (( term > best_term )); then best_term=$term; best_idx=$idx; fi
  done
  echo "$best_idx"
}

# wait_for_leader <partition> [timeout_s=8]
wait_for_leader() {
  local p="$1" timeout="${2:-8}" rf=$CLUSTER_RF
  local waited=0
  while true; do
    for ((r=0; r<rf; r++)); do
      local idx=$((p*rf+r))
      grep -q "became LEADER" "$LOGDIR/server-$idx.log" 2>/dev/null && return 0
    done
    sleep 0.5; waited=$((waited+1))
    (( waited * 5 >= timeout * 10 )) && return 1
  done
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 1 — All 5 KV operations (RF=1, 1 partition)
# ─────────────────────────────────────────────────────────────────────────────
run_test_1() {
  hdr 1 "All 5 KV operations (PUT/GET/SWAP/DELETE/SCAN)  [RF=1, 1 partition]"
  start_cluster 1 1

  assert_eq "PUT new key → not_found" \
    "PUT k1 not_found" "$(kvcmd 'PUT k1 hello')"

  assert_eq "PUT overwrite → found" \
    "PUT k1 found" "$(kvcmd 'PUT k1 hello2')"

  assert_eq "GET existing key" \
    "GET k1 hello2" "$(kvcmd 'GET k1')"

  assert_eq "GET missing key → null" \
    "GET missing null" "$(kvcmd 'GET missing')"

  assert_eq "SWAP existing → returns old value" \
    "SWAP k1 hello2" "$(kvcmd 'SWAP k1 swapped')"

  assert_eq "GET after SWAP → new value" \
    "GET k1 swapped" "$(kvcmd 'GET k1')"

  assert_eq "SWAP on missing key → null" \
    "SWAP newk null" "$(kvcmd 'SWAP newk nv')"

  assert_eq "GET after SWAP-on-missing → value set" \
    "GET newk nv" "$(kvcmd 'GET newk')"

  assert_eq "DELETE existing → found" \
    "DELETE k1 found" "$(kvcmd 'DELETE k1')"

  assert_eq "GET after DELETE → null" \
    "GET k1 null" "$(kvcmd 'GET k1')"

  assert_eq "DELETE missing → not_found" \
    "DELETE k1 not_found" "$(kvcmd 'DELETE k1')"

  # SCAN: insert a..e, then scan b..d (inclusive)
  kvcmd 'PUT a 1' 'PUT b 2' 'PUT c 3' 'PUT d 4' 'PUT e 5' > /dev/null
  local scan_out expected
  scan_out="$(kvcmd 'SCAN b d')"
  expected="$(printf 'SCAN b d BEGIN\n  b 2\n  c 3\n  d 4\nSCAN END')"
  assert_eq "SCAN [b,d] inclusive" "$expected" "$scan_out"

  kill_all
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 2 — Multi-partition key routing (RF=1, 2 partitions)
# ─────────────────────────────────────────────────────────────────────────────
run_test_2() {
  hdr 2 "Multi-partition key routing  [RF=1, 2 partitions]"
  start_cluster 2 1

  local keys=("apple" "banana" "cherry" "date" "elderberry" "fig" "grape" "honeydew")
  for k in "${keys[@]}"; do
    kvcmd "PUT $k val_$k" > /dev/null
  done

  local all_pass=true
  for k in "${keys[@]}"; do
    local got; got="$(kvcmd "GET $k")"
    if [[ "$got" != "GET $k val_$k" ]]; then
      fail "GET $k: expected 'GET $k val_$k'  got='$got'"
      all_pass=false
    fi
  done
  $all_pass && pass "All ${#keys[@]} keys routed and retrieved correctly"

  # Client-side SCAN must merge from both partitions and return sorted
  local scan_out scan_keys expected_keys
  scan_out="$(kvcmd 'SCAN a z')"
  scan_keys="$(echo "$scan_out" | grep '^  ' | awk '{print $1}')"
  expected_keys="$(printf '%s\n' "${keys[@]}" | sort)"
  assert_eq "SCAN a-z merges all keys from both partitions" \
    "$expected_keys" "$scan_keys"

  kill_all
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 3 — Leader election after leader crash (RF=3)
# ─────────────────────────────────────────────────────────────────────────────
run_test_3() {
  hdr 3 "Leader election after leader crash  [RF=3, 1 partition]"
  start_cluster 1 3

  kvcmd 'PUT before_crash yes' > /dev/null

  local li; li=$(leader_idx 0)
  log "Killing leader (server $li)..."
  kill_server "$li"

  log "Waiting for new election..."
  sleep 5   # Raft election timeout is typically 150-300ms; 5s is ample

  # Retry up to 5 times: the new leader may not have applied all log entries
  # (including its own no-op) the instant it wins election.
  local got_bc=""
  for ((retry=0; retry<5; retry++)); do
    got_bc="$(kvcmd 'GET before_crash')"
    [[ "$got_bc" == "GET before_crash yes" ]] && break
    sleep 1
  done
  assert_eq "Pre-crash data survives failover" "GET before_crash yes" "$got_bc"

  assert_eq "Cluster accepts writes after failover" \
    "PUT after_crash not_found" "$(kvcmd 'PUT after_crash yes')"

  assert_eq "Post-failover write is readable" \
    "GET after_crash yes" "$(kvcmd 'GET after_crash')"

  kill_all
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 4 — Follower crash + Raft catch-up (RF=3)
# ─────────────────────────────────────────────────────────────────────────────
run_test_4() {
  hdr 4 "Follower crash + catch-up via log replication  [RF=3, 1 partition]"
  start_cluster 1 3

  kvcmd 'PUT early a' > /dev/null

  local li; li=$(leader_idx 0)
  local follower=$(( (li + 1) % 3 ))
  log "Leader=$li  Killing follower=$follower"
  kill_server "$follower"

  # 2/3 = majority → leader can still commit
  kvcmd 'PUT while_down b' 'PUT while_down2 c' > /dev/null

  # Bring follower back — it must catch up via AppendEntries
  log "Restarting follower=$follower..."
  restart_server "$follower"
  sleep 4

  # Now kill the original leader to force the rejoined follower to serve
  log "Killing original leader=$li to force follower to serve..."
  kill_server "$li"
  sleep 5

  assert_eq "Data written before follower crashed" \
    "GET early a" "$(kvcmd 'GET early')"

  assert_eq "Data written while follower was down (1)" \
    "GET while_down b" "$(kvcmd 'GET while_down')"

  assert_eq "Data written while follower was down (2)" \
    "GET while_down2 c" "$(kvcmd 'GET while_down2')"

  kill_all
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 5 — Minority cannot make progress (RF=3, quorum lost)
# ─────────────────────────────────────────────────────────────────────────────
run_test_5() {
  hdr 5 "Minority cannot make progress — no quorum  [RF=3, kill 2 of 3]"
  start_cluster 1 3

  kvcmd 'PUT before zzz' > /dev/null

  local li; li=$(leader_idx 0)
  local f1=$(( (li + 1) % 3 ))
  local f2=$(( (li + 2) % 3 ))
  log "Killing 2 of 3 servers (keeping only leader=$li)"
  kill_server "$f1"; kill_server "$f2"
  sleep 1

  log "Attempting write with no quorum — should block..."
  local timed_out=false
  ( printf 'PUT no_quorum val\nSTOP\n' \
      | "$BIN/kvclient" --manager_addrs "$MANAGERS" 2>/dev/null ) &
  local cpid=$!
  sleep 6
  if kill -0 "$cpid" 2>/dev/null; then
    kill "$cpid" 2>/dev/null; timed_out=true
  fi
  wait "$cpid" 2>/dev/null || true

  if $timed_out; then
    pass "Write correctly blocked with only 1/3 alive"
  else
    fail "Write should have blocked (split-brain risk)"
  fi

  # Restore one node → quorum = 2/3
  log "Restoring node $f1 (quorum restored)..."
  restart_server "$f1"
  sleep 5

  assert_eq "Pre-partition data readable after quorum restored" \
    "GET before zzz" "$(kvcmd 'GET before')"

  local wr_after; wr_after="$(kvcmd 'PUT after_restore ok')"
  assert_eq "Write succeeds after quorum restored" \
    "PUT after_restore not_found" "$wr_after"

  kill_all
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 6 — Full cluster restart — persistence (RF=3)
# ─────────────────────────────────────────────────────────────────────────────
run_test_6() {
  hdr 6 "Full cluster restart — persistence  [RF=3, 1 partition]"
  start_cluster 1 3

  for i in $(seq 1 10); do
    kvcmd "PUT persist_$i val_$i" > /dev/null
  done
  log "Wrote 10 keys — killing entire cluster..."

  # Hard-kill all nodes; backer dirs remain intact under TESTDIR
  kill_all
  sleep 0.5

  log "Restarting cluster from persisted state..."
  restart_cluster
  log "Cluster restarted — verifying data..."

  local all_ok=true
  for i in $(seq 1 10); do
    local got; got="$(kvcmd "GET persist_$i")"
    if [[ "$got" != "GET persist_$i val_$i" ]]; then
      fail "Persistence: persist_$i (got='$got')"
      all_ok=false
    fi
  done
  $all_ok && pass "All 10 keys survived a full cluster restart"

  kill_all
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 7 — Concurrent clients — no lost writes (RF=3)
# ─────────────────────────────────────────────────────────────────────────────
run_test_7() {
  hdr 7 "Concurrent clients — no lost writes  [RF=3, 5 parallel clients]"
  start_cluster 1 3

  local NCLIS=5 NKEYS=10 pids=()
  for ((c=0; c<NCLIS; c++)); do
    (
      for ((k=0; k<NKEYS; k++)); do echo "PUT cli${c}_key${k} val_${c}_${k}"; done
      echo "STOP"
    ) | "$BIN/kvclient" --manager_addrs "$MANAGERS" > "$TESTDIR/cli-$c.log" 2>/dev/null &
    pids+=($!)
  done
  for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done

  local lost=0
  for ((c=0; c<NCLIS; c++)); do
    for ((k=0; k<NKEYS; k++)); do
      local got; got="$(kvcmd "GET cli${c}_key${k}")"
      [[ "$got" != "GET cli${c}_key${k} val_${c}_${k}" ]] && ((lost++))
    done
  done

  if [[ $lost -eq 0 ]]; then
    pass "All $((NCLIS*NKEYS)) writes from $NCLIS concurrent clients survived"
  else
    fail "$lost / $((NCLIS*NKEYS)) writes lost (concurrent clients)"
  fi

  kill_all
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 8 — RF=5 tolerates 2 simultaneous crashes
# ─────────────────────────────────────────────────────────────────────────────
run_test_8() {
  hdr 8 "RF=5 — tolerate 2 simultaneous crashes  [RF=5, 1 partition]"
  start_cluster 1 5

  kvcmd 'PUT before_crashes yes' > /dev/null

  local li; li=$(leader_idx 0)
  # Kill 2 non-leader replicas
  local victims=()
  for ((i=0; i<5; i++)); do
    [[ $i -eq $li ]] && continue
    victims+=("$i")
    [[ ${#victims[@]} -eq 2 ]] && break
  done
  log "Killing 2 followers: ${victims[*]} (leader=$li)"
  kill_server "${victims[0]}"; kill_server "${victims[1]}"
  sleep 1

  assert_eq "Write succeeds with 3/5 alive" \
    "PUT during_crashes not_found" "$(kvcmd 'PUT during_crashes yes')"

  assert_eq "Pre-crash data readable with 3/5 alive" \
    "GET before_crashes yes" "$(kvcmd 'GET before_crashes')"

  # Kill the leader → only 2/5 alive → below majority → must stall
  log "Killing leader $li → 2/5 alive, below quorum"
  kill_server "$li"
  sleep 1

  local timed_out=false
  ( printf 'PUT no_quorum5 val\nSTOP\n' \
      | "$BIN/kvclient" --manager_addrs "$MANAGERS" 2>/dev/null ) &
  local cpid=$!
  sleep 5
  if kill -0 "$cpid" 2>/dev/null; then kill "$cpid" 2>/dev/null; timed_out=true; fi
  wait "$cpid" 2>/dev/null || true

  if $timed_out; then
    pass "RF=5: write correctly stalls with 2/5 alive"
  else
    fail "RF=5: write should have stalled with 2/5 alive"
  fi

  kill_all
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 9 — SWAP atomicity: old value always correct (RF=3)
# ─────────────────────────────────────────────────────────────────────────────
run_test_9() {
  hdr 9 "SWAP atomicity — old value always returned correctly  [RF=3]"
  start_cluster 1 3

  kvcmd 'PUT chain start' > /dev/null

  local prev="start"
  for step in alpha beta gamma delta epsilon; do
    local out got_old
    out="$(kvcmd "SWAP chain $step")"
    got_old="$(echo "$out" | awk '{print $3}')"
    if [[ "$got_old" == "$prev" ]]; then
      pass "SWAP chain [$step]: old='$prev' ✓"
    else
      fail "SWAP chain [$step]: expected old='$prev' got='$got_old'"
    fi
    prev="$step"
  done

  assert_eq "GET after swap chain → final value" \
    "GET chain epsilon" "$(kvcmd 'GET chain')"

  assert_eq "SWAP on missing key → null" \
    "SWAP brand_new null" "$(kvcmd 'SWAP brand_new first_val')"

  assert_eq "Key created by SWAP-on-missing is readable" \
    "GET brand_new first_val" "$(kvcmd 'GET brand_new')"

  kill_all
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 10 — SCAN across partitions (RF=1, 2 partitions)
# ─────────────────────────────────────────────────────────────────────────────
run_test_10() {
  hdr 10 "SCAN merges results across 2 partitions  [RF=1, 2 partitions]"
  start_cluster 2 1

  local all_keys=("aardvark" "buffalo" "crane" "dingo" "eagle" "falcon" "gnu" "hyena")
  for k in "${all_keys[@]}"; do
    kvcmd "PUT $k val_$k" > /dev/null
  done

  # Full SCAN must return all keys sorted
  local scan_out scan_keys want_keys
  scan_out="$(kvcmd 'SCAN a z')"
  scan_keys="$(echo "$scan_out" | grep '^  ' | awk '{print $1}')"
  want_keys="$(printf '%s\n' "${all_keys[@]}" | sort)"
  assert_eq "SCAN a-z returns all keys from both partitions sorted" \
    "$want_keys" "$scan_keys"

  # Partial range scan: [buffalo, falcon] — both are the full key names so the
  # range boundary comparison is exact. crane, dingo, eagle are between them.
  local partial_out partial_keys want_partial
  partial_out="$(kvcmd 'SCAN buffalo falcon')"
  partial_keys="$(echo "$partial_out" | grep '^  ' | awk '{print $1}')"
  want_partial="$(printf '%s\n' "buffalo" "crane" "dingo" "eagle" "falcon" | sort)"
  assert_eq "SCAN [buffalo,falcon] inclusive returns 5 in-range keys" \
    "$want_partial" "$partial_keys"

  # DELETE one key → should vanish from next SCAN
  kvcmd 'DELETE crane' > /dev/null
  local after_del del_keys want_after
  after_del="$(kvcmd 'SCAN a z')"
  del_keys="$(echo "$after_del" | grep '^  ' | awk '{print $1}')"
  want_after="$(printf '%s\n' "${all_keys[@]}" | grep -v '^crane$' | sort)"
  assert_eq "SCAN after DELETE excludes removed key" \
    "$want_after" "$del_keys"

  kill_all
}

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
echo -e "\n${BOLD}${CYAN}P3 Custom Test Suite${RESET}"
echo -e "${CYAN}  Binary : $BIN${RESET}"
echo -e "${CYAN}  Scratch: $TESTDIR${RESET}"
echo -e "${CYAN}  Tests  : ${SELECTED[*]:-all}${RESET}\n"

for exe in kvserver kvclient kvmanager; do
  if [[ ! -x "$BIN/$exe" ]]; then
    echo -e "${RED}ERROR: $BIN/$exe not found. Run 'just p3::build' first.${RESET}"
    exit 1
  fi
done

# Global initial cleanup — kills any stray processes from a previous run
log "Global cleanup before tests..."
kill_all
rm -rf "$TESTDIR"

for n in 1 2 3 4 5 6 7 8 9 10; do
  if should_run $n; then
    "run_test_$n"
    kill_all   # defensive cleanup between tests
    sleep 0.5
  else
    skip "Test $n (not selected)"
  fi
done

echo ""
echo -e "${BOLD}══════════════════════════════════════${RESET}"
echo -e "${BOLD}  ${GREEN}${PASS} passed${RESET}  ${RED}${FAIL} failed${RESET}  ${YELLOW}${SKIP} skipped${RESET}"
echo -e "${BOLD}══════════════════════════════════════${RESET}"
echo ""
if [[ $FAIL -eq 0 ]]; then
  echo -e "${GREEN}${BOLD}ALL TESTS PASSED${RESET}"
  exit 0
else
  echo -e "${RED}${BOLD}${FAIL} TEST(S) FAILED — logs: $LOGDIR${RESET}"
  exit 1
fi
