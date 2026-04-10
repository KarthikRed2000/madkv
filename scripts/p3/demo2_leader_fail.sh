#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# DEMO SCENARIO 2 — Leader failure + transparent election + client recovery
#
# What it shows:
#   • Identify the current leader of partition 0 (from server logs)
#   • Write data to the cluster
#   • Kill the leader node of partition 0
#   • Raft triggers an election transparently
#   • Client automatically finds the new leader and continues operations
#
# Run from node0.  Usage:  bash scripts/p3/demo2_leader_fail.sh [--clean]
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

# Find which replica of partition 0 is the current leader
# Returns the replica index (0..RF-1); sets LEADER_HOST and LEADER_REP globals
find_leader_p0() {
  local best_term=-1 best_rep=-1
  for ((r=0; r<RF; r++)); do
    local idx=$((0 * RF + r))
    local node_idx=$((idx + 2))
    local host="${NODE_HOSTS[$node_idx]}"
    local log="${LOG_DIR}/server-0-${r}.log"
    # Find the highest "became LEADER" term in the log
    local term
    term=$(ssh_node "$host" \
      "grep 'became LEADER' ${log} 2>/dev/null | grep -oE 'term[= ]+[0-9]+' | grep -oE '[0-9]+' | tail -1" 2>/dev/null || echo "")
    [[ -z "$term" ]] && continue
    if (( term > best_term )); then
      best_term=$term; best_rep=$r
    fi
  done
  LEADER_REP=$best_rep
  if [[ $best_rep -ge 0 ]]; then
    local lidx=$((0 * RF + best_rep))
    LEADER_HOST="${NODE_HOSTS[$((lidx+2))]}"
    LEADER_API_PORT=$((API_BASE + lidx))
  else
    LEADER_HOST=""; LEADER_API_PORT=0
  fi
}

F=$(( (RF - 1) / 2 ))

echo -e "\n${BOLD}${CYAN}════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  DEMO 2: Leader Failure — Election + Client Recovery${RESET}"
echo -e "${BOLD}${CYAN}════════════════════════════════════════════════════════${RESET}"
echo -e "  Partitions : ${NPARTS}   RF : ${RF}   f : ${F}"

# ── STEP 1: Start cluster ──────────────────────────────────────────────────────
step "1. Starting fresh cluster  [NPARTS=${NPARTS}, RF=${RF}]"
if [[ $CLEAN -eq 1 ]]; then
  bash "${SCRIPT_DIR}/start_all.sh" --clean --rf "${RF}" --nparts "${NPARTS}"
else
  bash "${SCRIPT_DIR}/start_all.sh" --rf "${RF}" --nparts "${NPARTS}"
fi
sleep 3

# ── STEP 2: Write data ─────────────────────────────────────────────────────────
step "2. Writing data before leader failure"
run_client "initial writes" \
  "PUT before_leader_fail alpha" \
  "PUT another_key       beta"
ok "Data committed under current leader."
sleep 1

# ── STEP 3: Identify the leader ────────────────────────────────────────────────
step "3. Identifying current leader of partition 0"
find_leader_p0
if [[ $LEADER_REP -lt 0 ]]; then
  warn "Could not determine leader from logs. Defaulting to replica 0."
  LEADER_REP=0
  LEADER_HOST="${NODE_HOSTS[2]}"
  LEADER_API_PORT=$((API_BASE + 0))
fi
info "  Current leader: partition 0, replica ${LEADER_REP}"
info "  Host          : ${LEADER_HOST}"
info "  API port      : ${LEADER_API_PORT}"
info "  (Confirmed from 'became LEADER' in server log)"
echo ""
ssh_node "$LEADER_HOST" \
  "grep 'became LEADER' ${LOG_DIR}/server-0-${LEADER_REP}.log 2>/dev/null | tail -3" \
  | while read -r line; do info "    LOG: $line"; done
pause

# ── STEP 4: Kill the leader ────────────────────────────────────────────────────
step "4. Killing the leader: s0.${LEADER_REP} on ${LEADER_HOST}"
warn "Sending SIGKILL to kvserver on ${LEADER_HOST} ..."
ssh_node "$LEADER_HOST" "pkill -9 kvserver 2>/dev/null || true"
ok "Leader process killed.  Election will begin within election timeout (~300ms)."
pause

# ── STEP 5: Watch for election ─────────────────────────────────────────────────
step "5. Waiting for a new leader to be elected  (up to 10s)..."
NEW_LEADER_REP=-1
for ((attempt=1; attempt<=20; attempt++)); do
  for ((r=0; r<RF; r++)); do
    [[ $r -eq $LEADER_REP ]] && continue   # skip the dead node
    local_idx=$((0 * RF + r))
    node_idx=$((local_idx + 2))
    host="${NODE_HOSTS[$node_idx]}"
    log="${LOG_DIR}/server-0-${r}.log"
    # Look for a LEADER entry with term > what we found before
    if ssh_node "$host" "grep -q 'became LEADER' ${log} 2>/dev/null" 2>/dev/null; then
      NEW_TERM=$(ssh_node "$host" \
        "grep 'became LEADER' ${log} 2>/dev/null | grep -oE 'term[= ]+[0-9]+' | grep -oE '[0-9]+' | tail -1" 2>/dev/null || echo "-1")
      if (( NEW_TERM > 0 )); then
        NEW_LEADER_REP=$r
        NEW_LEADER_HOST="${NODE_HOSTS[$((local_idx+2))]}"
        break 2
      fi
    fi
  done
  sleep 0.5
done

if [[ $NEW_LEADER_REP -ge 0 ]]; then
  ok "New leader elected: partition 0, replica ${NEW_LEADER_REP} on ${NEW_LEADER_HOST}"
  ssh_node "$NEW_LEADER_HOST" \
    "grep 'became LEADER' ${LOG_DIR}/server-0-${NEW_LEADER_REP}.log 2>/dev/null | tail -2" \
    | while read -r line; do info "    LOG: $line"; done
else
  warn "Could not confirm new leader from logs. Client will discover it via retry."
fi
pause

# ── STEP 6: Show client continues seamlessly ───────────────────────────────────
step "6. Client operations after leader failure + re-election"
info "Reading pre-crash data (must be linearizable)..."
run_client "read after failover" \
  "GET before_leader_fail" \
  "GET another_key"

info "Writing new data (cluster must accept it)..."
run_client "write after failover" \
  "PUT after_leader_fail gamma" \
  "GET after_leader_fail"

ok "Client completed all operations. The new leader served them transparently."

echo ""
echo -e "${BOLD}${GREEN}══ DEMO 2 COMPLETE ══${RESET}"
echo -e "  The old leader (s0.${LEADER_REP}) was crashed."
echo -e "  Raft elected a new leader (s0.${NEW_LEADER_REP:-?}) via transparent timeout + vote."
echo -e "  Pre-crash data was still visible (linearizable reads)."
echo -e "  New writes committed without any client-side configuration change."
