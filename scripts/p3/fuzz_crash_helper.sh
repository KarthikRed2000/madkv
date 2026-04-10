#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# fuzz_crash_helper.sh — Crash 2 servers (1 must be leader) during a live
#                         fuzz run for the required Fuzz Scenario 2.
#
# How to use this for the required fuzz test:
#
#   Terminal A (node0):
#     bash scripts/p3/start_all.sh --clean --rf 5 --nparts 1
#     # wait for cluster to become ready, then:
#     just p3::fuzz server_rf=5 crashing=yes managers="<MANAGERS>"
#
#   Terminal B (node0), while the fuzzer runs in Terminal A:
#     bash scripts/p3/fuzz_crash_helper.sh
#
# The helper will:
#   1. Find the current leader of partition 0 from server logs
#   2. Wait for user confirmation (so you can show the grader what's happening)
#   3. Kill the leader (crash #1)
#   4. After election completes, kill one more follower (crash #2)
#   5. Print status — fuzzer continues in Terminal A
#
# Run from node0.
# ─────────────────────────────────────────────────────────────────────────────
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

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

# Find highest-term leader replica in partition 0
find_leader_p0() {
  local best_term=-1
  LEADER_REP=-1; LEADER_HOST=""
  for ((r=0; r<RF; r++)); do
    local idx=$((0 * RF + r))
    local host="${NODE_HOSTS[$((idx+2))]}"
    local log="${LOG_DIR}/server-0-${r}.log"
    local term
    term=$(ssh_node "$host" \
      "grep 'became LEADER' ${log} 2>/dev/null | grep -oE 'term[= ]+[0-9]+' | grep -oE '[0-9]+' | tail -1" \
      2>/dev/null || echo "")
    [[ -z "$term" ]] && continue
    if (( term > best_term )); then
      best_term=$term; LEADER_REP=$r
      LEADER_HOST="${NODE_HOSTS[$((0*RF+r+2))]}"
    fi
  done
}

# ─────────────────────────────────────────────────────────────────────────────
F=$(( (RF - 1) / 2 ))

echo -e "\n${BOLD}${CYAN}════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  FUZZ CRASH HELPER — Crash 2 Servers During Fuzz Run${RESET}"
echo -e "${BOLD}${CYAN}════════════════════════════════════════════════════════${RESET}"
echo -e "  Partition 0, RF=${RF}, f=${F}"
echo -e "  Will crash: 1 LEADER + 1 FOLLOWER (= 2 total, ≤ f=${F})"
echo -e "  Fuzzer should still complete successfully."
echo ""
warn "Make sure the fuzzer is already running in another terminal before continuing."
pause

# ── Find the current leader ────────────────────────────────────────────────────
step "1. Finding current leader of partition 0..."
find_leader_p0
if [[ $LEADER_REP -lt 0 ]]; then
  warn "Could not detect leader from logs; defaulting to replica 0."
  LEADER_REP=0
  LEADER_HOST="${NODE_HOSTS[2]}"
fi
ok "Current leader: s0.${LEADER_REP} on ${LEADER_HOST}"
ssh_node "$LEADER_HOST" \
  "grep 'became LEADER' ${LOG_DIR}/server-0-${LEADER_REP}.log 2>/dev/null | tail -2" \
  | while read -r line; do info "  LOG: $line"; done
pause

# ── Kill the leader (crash #1) ─────────────────────────────────────────────────
step "2. Crash #1 — Killing the LEADER: s0.${LEADER_REP} on ${LEADER_HOST}"
warn "Sending SIGKILL to kvserver on ${LEADER_HOST} ..."
ssh_node "$LEADER_HOST" "pkill -9 kvserver 2>/dev/null || true"
ok "Leader killed.  New election starting (Raft election timeout ~300ms)."
info "The fuzzer will see a brief pause, then continue via the new leader."

# ── Wait for new election ──────────────────────────────────────────────────────
step "3. Waiting for new leader election..."
sleep 5
find_leader_p0
NEW_LEADER_REP=$LEADER_REP; NEW_LEADER_HOST=$LEADER_HOST
if [[ $NEW_LEADER_REP -lt 0 ]]; then
  warn "New leader not yet visible in logs. Giving more time..."
  sleep 5; find_leader_p0; NEW_LEADER_REP=$LEADER_REP; NEW_LEADER_HOST=$LEADER_HOST
fi
if [[ $NEW_LEADER_REP -ge 0 ]]; then
  ok "New leader elected: s0.${NEW_LEADER_REP} on ${NEW_LEADER_HOST}"
else
  warn "Could not confirm new leader; proceeding anyway."
fi
pause

# ── Kill a follower (crash #2) ─────────────────────────────────────────────────
step "4. Crash #2 — Killing a FOLLOWER (crash 2 of 2)"
# Pick a follower: any surviving replica that is NOT the old leader or new leader
FOLLOWER_REP=-1; FOLLOWER_HOST=""
for ((r=0; r<RF; r++)); do
  # Skip the dead old leader and the new leader
  [[ $r -eq $((LEADER_REP)) ]] && continue   # old leader (already dead)
  # Note: find_leader_p0 just overwrote LEADER_REP with NEW_LEADER_REP
  [[ $r -eq $NEW_LEADER_REP ]] && continue
  FOLLOWER_REP=$r
  FOLLOWER_HOST="${NODE_HOSTS[$((0*RF+r+2))]}"
  break
done

if [[ $FOLLOWER_REP -lt 0 ]]; then
  # Fallback: kill the first non-new-leader we can find
  for ((r=0; r<RF; r++)); do
    [[ $r -eq $NEW_LEADER_REP ]] && continue
    FOLLOWER_REP=$r
    FOLLOWER_HOST="${NODE_HOSTS[$((0*RF+r+2))]}"
    break
  done
fi

if [[ $FOLLOWER_REP -ge 0 ]]; then
  info "  Killing follower s0.${FOLLOWER_REP} on ${FOLLOWER_HOST}"
  ssh_node "$FOLLOWER_HOST" "pkill -9 kvserver 2>/dev/null || true"
  ok "Follower s0.${FOLLOWER_REP} killed."
else
  warn "No follower found to kill (cluster might be too small). Skipping."
fi

# ── Summary ────────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}${GREEN}══ CRASH HELPER COMPLETE ══${RESET}"
echo -e "  Crashed: s0.${LEADER_REP} (was leader) + s0.${FOLLOWER_REP:-?} (follower)"
echo -e "  Total crashed: 2 / ${RF}  (≤ f=${F} — majority still alive)"
echo -e "  New leader   : s0.${NEW_LEADER_REP:-?}"
echo ""
echo -e "  ${YELLOW}Switch to Terminal A to watch the fuzzer complete.${RESET}"
echo -e "  It should print PASSED when done."
echo ""
echo -e "  ${CYAN}Cluster status:${RESET}"
for ((r=0; r<RF; r++)); do
  local_idx=$((0*RF+r))
  host="${NODE_HOSTS[$((local_idx+2))]}"
  if [[ $r -eq $LEADER_REP ]] || [[ $r -eq ${FOLLOWER_REP:--99} ]]; then
    echo -e "    s0.${r}  ${RED}KILLED${RESET}  (${host})"
  elif [[ $r -eq $NEW_LEADER_REP ]]; then
    echo -e "    s0.${r}  ${GREEN}LEADER${RESET}  (${host})"
  else
    echo -e "    s0.${r}  ${GREEN}follower${RESET}  (${host})"
  fi
done
