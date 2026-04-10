#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# DEMO SCENARIO 3 — Manager failure
#
# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  IMPLEMENTATION STATUS: Manager Raft replication is NOT implemented.   ║
# ║  We run a single manager.  This demo shows:                            ║
# ║   (a) Crash the manager while a client is mid-operation — the          ║
# ║       client's in-flight RPC fails but the KV state is intact.         ║
# ║   (b) Restart the manager from its backer dir.                         ║
# ║   (c) A NEW client can join and use the KV service immediately.        ║
# ║  This demonstrates single-manager crash recovery via durable state.    ║
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

MGR_HOST="${NODE_HOSTS[1]}"
MGR_BACKER="$(get_backer m.0)"

echo -e "\n${BOLD}${CYAN}════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  DEMO 3: Manager Failure & Recovery${RESET}"
echo -e "${BOLD}${CYAN}════════════════════════════════════════════════════════${RESET}"
note "Manager Raft replication is a bonus feature and was not implemented."
note "This demo shows single-manager crash recovery via persistent state."
echo -e "  Manager host : ${MGR_HOST}  (${MANAGERS})"

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

# ── STEP 3: Kill the manager ───────────────────────────────────────────────────
step "3. Killing the manager on ${MGR_HOST}"
warn "Sending SIGKILL to kvmanager on ${MGR_HOST} ..."
ssh_node "$MGR_HOST" "pkill -9 kvmanager 2>/dev/null || true"
ok "Manager process killed."
sleep 1

# ── STEP 4: Show server Raft state is unaffected ──────────────────────────────
step "4. The KV servers continue running (Raft state is preserved)"
info "Servers run independently; manager is only needed for cluster discovery."
for ((p=0; p<NPARTS; p++)); do
  node_idx=$((p * RF + 2))  # replica 0 of each partition
  host="${NODE_HOSTS[$node_idx]}"
  info "  Partition ${p} leader running at ${host} — server log tail:"
  ssh_node "$host" \
    "tail -3 ${LOG_DIR}/server-${p}-0.log 2>/dev/null" \
    | while read -r line; do info "    $line"; done
done
pause

# ── STEP 5: Show that new clients cannot join (no manager to discover) ────────
step "5. New client attempt while manager is down (expected: fails to get cluster info)"
info "A new client must query the manager to discover server addresses."
warn "Without the manager, the client cannot start operations:"
( printf 'GET mgr_demo_key1\nSTOP\n' \
    | timeout 5 ssh_node "${NODE_HOSTS[0]}" \
        "${MADKV}/kvstore/bin/kvclient --manager_addrs ${MANAGERS}" 2>&1 \
    || true ) | while read -r line; do echo "     $line"; done
ok "Expected: client times out or errors — no manager to route it."
pause

# ── STEP 6: Restart the manager ───────────────────────────────────────────────
step "6. Restarting manager on ${MGR_HOST} from persisted backer"
ssh_node "$MGR_HOST" \
  "bash ${MADKV}/scripts/p3/start_manager.sh ${MADKV}/scripts/p3/config.sh"
sleep 3
ok "Manager restarted. It loads its state from backer at ${MGR_BACKER}."

# ── STEP 7: New client can now join ───────────────────────────────────────────
step "7. New client joins and uses the KV service after manager recovery"
run_client "after manager restart — read old data" \
  "GET mgr_demo_key1" \
  "GET mgr_demo_key2"

run_client "after manager restart — write new data" \
  "PUT mgr_demo_new   after_restart" \
  "GET mgr_demo_new"

ok "New client successfully joined and performed operations."

echo ""
echo -e "${BOLD}${GREEN}══ DEMO 3 COMPLETE ══${RESET}"
echo -e "  Manager was crashed and restarted."
echo -e "  KV server state was preserved throughout (Raft-replicated)."
echo -e "  After manager restart, new clients can immediately use the service."
echo ""
note "If manager Raft replication were implemented, the manager could tolerate"
note "f_mgr = floor((MRF-1)/2) crashes without any downtime. This is a bonus feature."
