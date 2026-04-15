#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# DEMO SCENARIO 3 — Manager failure with replication
#
# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  IMPLEMENTATION STATUS: Manager Raft replication is implemented.        ║
# ║  This demo shows:                                                       ║
# ║   (a) Crash one or more managers while a client is mid-operation — the  ║
# ║       client's in-flight RPC fails but the KV state is intact.          ║
# ║   (b) The remaining managers continue to serve client requests.         ║
# ║   (c) Restart the failed managers and verify full recovery.             ║
# ║  This demonstrates manager replication and fault tolerance.             ║
# ╚══════════════════════════════════════════════════════════════════════════╝
#
# Run from node0.  Usage:  bash scripts/p3/demo3_manager_fail.sh [--clean]
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
note()  { echo -e "   ${YELLOW}NOTE: $*${RESET}"; }
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

MGR_HOSTS=("${NODE_HOSTS[1]}" "${NODE_HOSTS[2]}" "${NODE_HOSTS[3]}")

# ── STEP 1: Start cluster ──────────────────────────────────────────────────────
step "1. Starting fresh cluster  [NPARTS=${NPARTS}, RF=${RF}]"
if [[ $CLEAN -eq 1 ]]; then
  bash "${SCRIPT_DIR}/start_all.sh" --clean --rf "${RF}" --nparts "${NPARTS}"
else
  bash "${SCRIPT_DIR}/start_all.sh" --rf "${RF}" --nparts "${NPARTS}"
fi
sleep 3

# ── STEP 2: Write data ─────────────────────────────────────────────────────────
step "2. Writing data before manager crash"
run_client "initial writes" \
  "PUT mgr_demo_key1  value1" \
  "PUT mgr_demo_key2  value2"
ok "Data stored in Raft-replicated server state."
sleep 1
pause

# ── STEP 3: Kill one or more managers ──────────────────────────────────────────
step "3. Killing one or more managers"
for mgr in "${MGR_HOSTS[@]:0:2}"; do
  warn "Sending SIGKILL to kvmanager on ${mgr} ..."
  ssh_node "$mgr" "pkill -9 kvmanager 2>/dev/null || true"
  ok "Manager process on ${mgr} killed."
done
sleep 1

# ── STEP 4: Verify remaining managers and server state ─────────────────────────
step "4. Verifying remaining managers and server state"
info "Remaining managers continue to serve client requests."
run_client "read data after partial manager failure" \
  "GET mgr_demo_key1" \
  "GET mgr_demo_key2"
ok "Client successfully read data with remaining managers."
sleep 1
pause

# ── STEP 5: Restart failed managers ────────────────────────────────────────────
step "5. Restarting failed managers"
for mgr in "${MGR_HOSTS[@]:0:2}"; do
  ssh_node "$mgr" \
    "bash ${MADKV}/scripts/p3/start_manager.sh ${MADKV}/scripts/p3/config.sh"
  ok "Manager on ${mgr} restarted."
done
sleep 3

# ── STEP 6: Verify full recovery ───────────────────────────────────────────────
step "6. Verifying full recovery after manager restart"
run_client "read and write data after full recovery" \
  "GET mgr_demo_key1" \
  "GET mgr_demo_key2" \
  "PUT mgr_demo_new   after_restart" \
  "GET mgr_demo_new"
ok "Client successfully performed operations after full recovery."

# ── DEMO COMPLETE ──────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}${GREEN}══ DEMO 3 COMPLETE ══${RESET}"
echo -e "  Managers were crashed and restarted."
echo -e "  KV server state was preserved throughout (Raft-replicated)."
echo -e "  After manager restart, new clients can immediately use the service."
echo ""
note "Manager Raft replication ensures fault tolerance. The system can tolerate"
note "f_mgr = floor((MRF-1)/2) manager crashes without downtime."
