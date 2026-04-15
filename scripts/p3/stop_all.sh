#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# stop_all.sh — Kill all P3 processes on every node in the cluster.
#
# Run from node0 (or any node with SSH access to the others).
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CFG="${SCRIPT_DIR}/config.sh"
source "$CFG"

log() { echo "[$(date '+%H:%M:%S')] $1"; }

kill_remote() {
  local host="$1"
  # IMPORTANT: do NOT use pkill -f here — the remote shell's argv contains "kvserver"
  # so "pkill -9 -f kvserver" would kill the shell itself, aborting the SSH session.
  # Matching by process name (without -f) is sufficient since the binaries are named
  # kvserver / kvmanager / kvclient.
  local kill_cmd="pkill -9 kvmanager 2>/dev/null; pkill -9 kvserver 2>/dev/null; pkill -9 kvclient 2>/dev/null; true"
  for attempt in 1 2 3; do
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
        -o PasswordAuthentication=no -o LogLevel=ERROR \
        "${CLOUDLAB_USER}@${host}" "$kill_cmd" 2>/dev/null && return
    sleep 1
  done
  true
}

log "Killing processes on all nodes..."

# Collect unique hosts to avoid redundant SSH calls
declare -A SEEN_HOSTS

# Manager replica nodes
for ((m=0; m<MGR_RF; m++)); do
  h="${MGR_HOSTS[$m]}"
  if [[ -z "${SEEN_HOSTS[$h]+x}" ]]; then
    SEEN_HOSTS[$h]=1
    kill_remote "$h" &
  fi
done

# Server nodes (index 2 .. 1+NPARTS*RF)
for ((i=0; i < NPARTS*RF; i++)); do
  h="${NODE_HOSTS[$((i+2))]}"
  if [[ -z "${SEEN_HOSTS[$h]+x}" ]]; then
    SEEN_HOSTS[$h]=1
    kill_remote "$h" &
  fi
done

# Also kill locally (node0 may run clients)
pkill -9 kvmanager 2>/dev/null || true
pkill -9 kvserver  2>/dev/null || true
pkill -9 kvclient  2>/dev/null || true

wait

# Verification pass — warn if anything is still alive
log "Verifying all processes are gone..."
any_left=0
for h in "${!SEEN_HOSTS[@]}"; do
  cnt=$(ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 \
           -o PasswordAuthentication=no -o LogLevel=ERROR \
           "${CLOUDLAB_USER}@${h}" \
           "pgrep kvserver 2>/dev/null | wc -l | tr -d ' '" 2>/dev/null || echo "?")
  if [[ "$cnt" != "0" && "$cnt" != "" && "$cnt" != "?" ]]; then
    echo "  WARNING: $h still has $cnt kvserver process(es) — running extra kill..."
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
        -o PasswordAuthentication=no -o LogLevel=ERROR \
        "${CLOUDLAB_USER}@${h}" \
        "pkill -9 kvserver 2>/dev/null; pkill -9 kvmanager 2>/dev/null; true" 2>/dev/null || true
    any_left=1
  fi
done
[[ $any_left -eq 1 ]] && sleep 2

log "All P3 processes stopped."
