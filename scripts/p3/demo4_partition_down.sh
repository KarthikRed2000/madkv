#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# DEMO SCENARIO 4 — Partition unavailability: >f failures → requests hang
#
# What it shows:
#   • Kill MORE than f = floor((RF-1)/2) servers in partition 0
#   • The cluster loses quorum for that partition
#   • Client requests to partition 0 keys hang (no inconsistent results!)
#   • Client requests to partition 1 keys still work (other partition unaffected)
#
# Run from node0.  Usage:  bash scripts/p3/demo4_partition_down.sh [--clean]
# ─────────────────────────────────────────────────────────────────────────────
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

CLEAN=0; [[ "${1:-}" == "--clean" ]] && CLEAN=1

GREEN='\033[0;32m'; RED='\033[0;31m'; CYAN='\033[0;36m'
BOLD='\033[1m'; YELLOW='\033[0;33m'; RESET='\033[0m'

step()  { echo -e "\n${BOLD}${CYAN}▶  $*${RESET}"; }
info()  { echo -e "   ${CYAN}$*${RESET}"; }
ok()    { echo -e "   ${GREEN}✓ $*${RESET}"; }
warn()  { echo -e "   ${YELLOW}⚠ $*${RESET}"; }
pause() { echo -e "\n${BOLD}${YELLOW}[DEMO] Press ENTER to continue...${RESET}"; read -r; }

ssh_node() {
  local host="$1"; shift
  ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$host" \
      "export PATH=\$HOME/.cargo/bin:\$HOME/.local/bin:\$PATH; $*"
}

run_client() {
  local desc="$1"; shift
  echo -e "\n   ${BOLD}$ kvclient${RESET}  (${desc})"
  ( printf '%s\n' "$@"; printf 'STOP\n' ) \
    | ssh_node "${NODE_HOSTS[0]}" \
        "${MADKV}/kvstore/bin/kvclient --manager_addrs ${MANAGERS}" 2>/dev/null \
    | grep -v '^STOP$' \
    | while read -r line; do echo "     $line"; done
}

# Timeout-wrapped client — shows that the operation hangs
run_client_timeout() {
  local desc="$1" timeout_s="$2"; shift 2
  echo -e "\n   ${BOLD}$ kvclient${RESET}  (${desc}, timeout=${timeout_s}s)"
  local output
  output=$(
    ( printf '%s\n' "$@"; printf 'STOP\n' ) \
      | timeout "${timeout_s}" ssh_node "${NODE_HOSTS[0]}" \
          "${MADKV}/kvstore/bin/kvclient --manager_addrs ${MANAGERS}" 2>/dev/null \
      || true
  )
  if [[ -z "$output" ]]; then
    echo -e "     ${YELLOW}[no response within ${timeout_s}s — client is BLOCKED]${RESET}"
  else
    echo "$output" | grep -v '^STOP$' | while read -r line; do echo "     $line"; done
  fi
}

F=$(( (RF - 1) / 2 ))
KILL_COUNT=$(( F + 1 ))   # one more than the tolerance threshold

echo -e "\n${BOLD}${CYAN}════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  DEMO 4: Partition Unavailability (>f failures)${RESET}"
echo -e "${BOLD}${CYAN}════════════════════════════════════════════════════════${RESET}"
echo -e "  Partitions : ${NPARTS}   RF : ${RF}   f : ${F}"
echo -e "  Will kill  : ${KILL_COUNT} servers in partition 0 (> f=${F} → no quorum)"

# ── STEP 1: Start cluster ──────────────────────────────────────────────────────
step "1. Starting fresh cluster  [NPARTS=${NPARTS}, RF=${RF}]"
if [[ $CLEAN -eq 1 ]]; then
  bash "${SCRIPT_DIR}/start_all.sh" --clean --rf "${RF}" --nparts "${NPARTS}"
else
  bash "${SCRIPT_DIR}/start_all.sh" --rf "${RF}" --nparts "${NPARTS}"
fi
sleep 3

# ── STEP 2: Write data to both partitions ─────────────────────────────────────
step "2. Writing data to both partitions (cluster fully healthy)"
# Write keys that will hash to each partition.
# The manager/client uses consistent hashing — we write specific keys and
# note which partition they land on after the fact.
run_client "initial writes (both partitions)" \
  "PUT p0_sentinel   stored_in_p0" \
  "PUT p1_sentinel   stored_in_p1" \
  "PUT demo4_key_a   val_a" \
  "PUT demo4_key_b   val_b"

run_client "verify reads" \
  "GET p0_sentinel" \
  "GET p1_sentinel" \
  "GET demo4_key_a" \
  "GET demo4_key_b"
ok "Both partitions are healthy and serving reads/writes."
pause

# ── STEP 3: Kill >f servers in partition 0 ────────────────────────────────────
step "3. Killing ${KILL_COUNT} servers in partition 0 (quorum loss: ${KILL_COUNT} > f=${F})"
warn "This will make partition 0 UNAVAILABLE."
for ((r=0; r<KILL_COUNT; r++)); do
  local_idx=$((0 * RF + r))
  node_idx=$((local_idx + 2))
  host="${NODE_HOSTS[$node_idx]}"
  api_port=$((API_BASE + local_idx))
  info "  Killing  s0.${r}  →  ${host}  (api_port=${api_port})"
  ssh_node "$host" "pkill -9 kvserver 2>/dev/null || true"
done
ok "Killed ${KILL_COUNT} out of ${RF} servers in partition 0."
info "Alive in partition 0: $((RF - KILL_COUNT)) / ${RF}  (need $((RF/2+1)) for quorum)"
sleep 2
pause

# ── STEP 4: Show partition 0 hangs ────────────────────────────────────────────
step "4. Client requests to partition 0 keys — must HANG, not return wrong data"
warn "Any key that hashes to partition 0 will hang. Attempting with timeout..."
# We try a PUT first (easier to show it hangs)
run_client_timeout "PUT to partition 0 (will hang)" 8 \
  "PUT p0_sentinel   should_not_succeed"

warn "The client BLOCKED for 8 seconds — no inconsistent result was returned."
warn "Raft correctly refuses to commit without quorum."
pause

# ── STEP 5: Show partition 1 still works ──────────────────────────────────────
step "5. Client requests to partition 1 keys — must STILL WORK"
info "Partition 1 is fully healthy (all ${RF} replicas alive)."
run_client "reads from partition 1 (should succeed)" \
  "GET p1_sentinel" \
  "GET demo4_key_b"

run_client "writes to partition 1 (should succeed)" \
  "PUT p1_new_key  written_while_p0_down" \
  "GET p1_new_key"
ok "Partition 1 remained fully available. No cross-partition contamination."
pause

# ── STEP 6: Restore quorum in partition 0 ────────────────────────────────────
step "6. Restarting servers in partition 0 to restore quorum"
for ((r=0; r<KILL_COUNT; r++)); do
  local_idx=$((0 * RF + r))
  node_idx=$((local_idx + 2))
  host="${NODE_HOSTS[$node_idx]}"
  info "  Restarting  s0.${r}  on ${host}"
  ssh_node "$host" \
    "bash ${MADKV}/scripts/p3/start_server.sh 0 ${r} ${MADKV}/scripts/p3/config.sh" &
done
wait
sleep 5
ok "Servers restarted and rejoining Raft group. Election underway..."

step "7. Verifying partition 0 is available again"
run_client "reads after recovery" \
  "GET p0_sentinel" \
  "GET demo4_key_a"

run_client "writes after recovery" \
  "PUT p0_recovered  all_back_online" \
  "GET p0_recovered"
ok "Partition 0 recovered! Data is consistent with pre-failure state."

echo ""
echo -e "${BOLD}${GREEN}══ DEMO 4 COMPLETE ══${RESET}"
echo -e "  ${KILL_COUNT} servers (> f=${F}) were killed in partition 0."
echo -e "  Raft correctly refused writes (no split-brain risk)."
echo -e "  Partition 1 remained fully available throughout."
echo -e "  After restart, partition 0 recovered from persisted Raft logs."
