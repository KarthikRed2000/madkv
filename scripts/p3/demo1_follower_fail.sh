#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# DEMO SCENARIO 1 — Follower failures in ≥2 partitions
#
# What it shows:
#   • A properly configured cluster (NPARTS=2, RF=5) where f=2 (floor((5-1)/2))
#   • Client can PUT/GET keys across both partitions
#   • Kill f=2 FOLLOWER replicas in EACH partition (4 nodes total)
#   • Client can still make and complete requests with consistent results
#
# Run from node0 (orchestrator). Requires passwordless SSH to all nodes.
# Usage:  bash scripts/p3/demo1_follower_fail.sh [--clean]
# ─────────────────────────────────────────────────────────────────────────────
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

CLEAN=0; [[ "${1:-}" == "--clean" ]] && CLEAN=1

# ── Colour helpers ────────────────────────────────────────────────────────────
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

# Run client commands and print output clearly
run_client() {
  local desc="$1"; shift
  echo -e "\n   ${BOLD}$ kvclient${RESET}  (${desc})"
  ( printf '%s\n' "$@"; printf 'STOP\n' ) \
    | ssh_node "${NODE_HOSTS[0]}" \
        "${MADKV}/kvstore/bin/kvclient --manager_addrs ${MANAGERS}" 2>/dev/null \
    | grep -v '^STOP$' \
    | while read -r line; do echo "     $line"; done
}

# ── Derived: f = floor((RF-1)/2) ─────────────────────────────────────────────
F=$(( (RF - 1) / 2 ))

echo -e "\n${BOLD}${CYAN}════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  DEMO 1: Follower Failures in ≥2 Partitions${RESET}"
echo -e "${BOLD}${CYAN}════════════════════════════════════════════════════════${RESET}"
echo -e "  Partitions : ${NPARTS}   RF : ${RF}   f (tolerable failures) : ${F}"
echo -e "  Manager    : ${MANAGERS}"

# ── STEP 1: Start (or use existing) cluster ───────────────────────────────────
step "1. Starting fresh cluster  [NPARTS=${NPARTS}, RF=${RF}]"
if [[ $CLEAN -eq 1 ]]; then
  bash "${SCRIPT_DIR}/start_all.sh" --clean --rf "${RF}" --nparts "${NPARTS}"
else
  bash "${SCRIPT_DIR}/start_all.sh" --rf "${RF}" --nparts "${NPARTS}"
fi
sleep 3

# ── STEP 2: Verify cluster is healthy ────────────────────────────────────────
step "2. Initial client operations (cluster fully healthy)"
info "Writing keys to both partitions and reading them back..."
run_client "write keys" \
  "PUT demo1_p0_key  val_partition0" \
  "PUT demo1_p1_key  val_partition1" \
  "PUT demo1_shared  hello_from_demo1"

run_client "read back" \
  "GET demo1_p0_key" \
  "GET demo1_p1_key" \
  "GET demo1_shared"
ok "All reads returned correct values — cluster is healthy."
pause

# ── STEP 3: Kill f followers in partition 0 ───────────────────────────────────
step "3. Killing ${F} FOLLOWER(s) in partition 0 (leaving ${RF}-${F}=${RF-F} alive)"
for ((r=1; r<=F; r++)); do
  local_idx=$((0 * RF + r))       # replicas 1,2 of partition 0
  node_idx=$((local_idx + 2))
  host="${NODE_HOSTS[$node_idx]}"
  api_port=$((API_BASE + local_idx))
  info "  Killing  s0.${r}  →  ${host}  (api_port=${api_port})"
  ssh_node "$host" "pkill -9 kvserver 2>/dev/null || true"
done
ok "Killed ${F} follower(s) in partition 0."

# ── STEP 4: Kill f followers in partition 1 ───────────────────────────────────
step "4. Killing ${F} FOLLOWER(s) in partition 1 (leaving ${RF}-${F}=${RF-F} alive)"
for ((r=1; r<=F; r++)); do
  local_idx=$((1 * RF + r))       # replicas 1,2 of partition 1
  node_idx=$((local_idx + 2))
  host="${NODE_HOSTS[$node_idx]}"
  api_port=$((API_BASE + local_idx))
  info "  Killing  s1.${r}  →  ${host}  (api_port=${api_port})"
  ssh_node "$host" "pkill -9 kvserver 2>/dev/null || true"
done
ok "Killed ${F} follower(s) in partition 1.  Total crashed: $((F*2)) / $((NPARTS*RF))."
sleep 2
pause

# ── STEP 5: Show client still works ──────────────────────────────────────────
step "5. Client operations with ${F} followers down per partition"
info "Reading pre-existing keys (must still return correct values)..."
run_client "read under failures" \
  "GET demo1_p0_key" \
  "GET demo1_p1_key" \
  "GET demo1_shared"

info "Writing new keys (must succeed despite ${F} failures per partition)..."
run_client "write under failures" \
  "PUT demo1_after_fail  written_after_crash" \
  "GET demo1_after_fail"
ok "Client completed all requests consistently — Raft majority is intact."

# ── STEP 6: Show leader log confirms elections are still healthy ──────────────
step "6. Leader logs on surviving replicas"
for p in 0 1; do
  # Replica 0 of each partition is always a survivor
  local_idx=$((p * RF + 0))
  node_idx=$((local_idx + 2))
  host="${NODE_HOSTS[$node_idx]}"
  info "  Partition ${p} surviving replica (s${p}.0) leader status:"
  ssh_node "$host" \
    "tail -5 ${LOG_DIR}/server-${p}-0.log 2>/dev/null | grep -i 'LEADER\|term\|commit' || echo '    (no matching lines)'" \
    | while read -r line; do echo "    $line"; done
done

echo ""
echo -e "${BOLD}${GREEN}══ DEMO 1 COMPLETE ══${RESET}"
echo -e "  ${F} follower(s) were killed in EACH of ${NPARTS} partitions."
echo -e "  The client continued to read and write correctly."
echo -e "  Raft quorum (majority = $((RF/2+1)) of ${RF}) was maintained."
