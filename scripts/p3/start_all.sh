#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# start_all.sh — Orchestrate a full P3 cluster across CloudLab nodes.
#
# Run this from node0 (the client/orchestrator machine).
# Prereqs:
#   • Passwordless SSH from node0 to all other nodes (CloudLab provides this).
#   • madkv repo built on every node: ~/madkv/kvstore/bin/kvserver etc.
#   • scripts/p3/config.sh filled in with your actual IPs and hostnames.
#
# Usage:
#   ./scripts/p3/start_all.sh [--clean] [--rf <1|3|5>] [--nparts <N>]
#
#   --clean  Wipe backer dirs on all nodes before starting (fresh state).
#   --rf N   Override RF from config.sh.
#   --nparts Override NPARTS from config.sh.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CFG="${SCRIPT_DIR}/config.sh"
# shellcheck source=config.sh
source "$CFG"

CLEAN=0
RF_OVERRIDE=""
NPARTS_OVERRIDE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --clean)         CLEAN=1;                  shift ;;
    --rf)            RF_OVERRIDE="$2";         shift 2 ;;
    --nparts)        NPARTS_OVERRIDE="$2";     shift 2 ;;
    *)               echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

# Re-source config to recompute derived values, then re-apply CLI overrides
source "$CFG"
[[ -n "$RF_OVERRIDE"     ]] && RF="$RF_OVERRIDE"
[[ -n "$NPARTS_OVERRIDE" ]] && NPARTS="$NPARTS_OVERRIDE"

# Recompute derived values that depend on RF / NPARTS
SERVER_ADDRS=()
for ((i=0; i < NPARTS*RF; i++)); do
  SERVER_ADDRS[$i]="${NODE_ADDRS[$((i+2))]}"
done
SERVERS=""
SERVER_P2PS=""
for ((i=0; i < NPARTS*RF; i++)); do
  [[ -n "$SERVERS" ]]     && SERVERS+=","
  [[ -n "$SERVER_P2PS" ]] && SERVER_P2PS+=","
  SERVERS+="${SERVER_ADDRS[$i]}:$((API_BASE + i))"
  SERVER_P2PS+="${SERVER_ADDRS[$i]}:$((P2P_BASE + i))"
done

PLOG="/tmp/madkv-p3/start_all.log"
mkdir -p "$(dirname "$PLOG")"

log() { echo "[$(date '+%H:%M:%S')] $1" | tee -a "$PLOG"; }

ssh_cmd() {
  local host="$1"; shift
  ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$host" \
      "export PATH=\$HOME/.cargo/bin:\$HOME/.local/bin:\$PATH; $*"
}

# Upload config.sh to a remote node
upload_config() {
  local host="$1"
  scp -o StrictHostKeyChecking=no "$CFG" "${host}:${MADKV}/scripts/p3/config.sh"
}

# ── Stop everything first ─────────────────────────────────────────────────────
log "Stopping any existing P3 processes..."
"${SCRIPT_DIR}/stop_all.sh" 2>/dev/null || true
sleep 1

# ── Optionally wipe backer directories ───────────────────────────────────────
if [[ $CLEAN -eq 1 ]]; then
  log "Cleaning backer directories on all nodes..."

  ssh_cmd "${NODE_HOSTS[1]}" "rm -rf /tmp/madkv-p3 && mkdir -p /tmp/madkv-p3/logs" &

  for ((p=0; p < NPARTS; p++)); do
    for ((r=0; r < RF; r++)); do
      idx=$((p * RF + r))
      ssh_cmd "${NODE_HOSTS[$((idx+2))]}" \
          "rm -rf /tmp/madkv-p3 && mkdir -p /tmp/madkv-p3/logs" &
    done
  done
  wait
  log "Backer directories cleaned."
fi

# ── Upload updated config + scripts to all nodes ─────────────────────────────
log "Uploading config.sh to all nodes..."
upload_script() {
  local host="$1" script="$2"
  scp -o StrictHostKeyChecking=no "${SCRIPT_DIR}/${script}" \
      "${host}:${MADKV}/scripts/p3/${script}" 2>/dev/null
}
upload_config "${NODE_HOSTS[1]}" &
upload_script  "${NODE_HOSTS[1]}" "start_manager.sh" &
for ((i=0; i < NPARTS*RF; i++)); do
  upload_config  "${NODE_HOSTS[$((i+2))]}" &
  upload_script  "${NODE_HOSTS[$((i+2))]}" "start_server.sh" &
done
wait

# ── Start manager (node1) ─────────────────────────────────────────────────────
log "Starting manager on ${NODE_HOSTS[1]} (${NODE_ADDRS[1]}:${MAN_PORT})..."
ssh_cmd "${NODE_HOSTS[1]}" \
    "OVERRIDE_RF='${RF}' OVERRIDE_SERVERS='${SERVERS}' \
     bash ${MADKV}/scripts/p3/start_manager.sh ${MADKV}/scripts/p3/config.sh"
sleep 1

# ── Start server replicas (nodes 2+) in parallel ─────────────────────────────
log "Starting ${NPARTS} partition(s) × RF=${RF} replicas..."
for ((p=0; p < NPARTS; p++)); do
  for ((r=0; r < RF; r++)); do
    idx=$((p * RF + r))
    node_idx=$((idx + 2))
    host="${NODE_HOSTS[$node_idx]}"
    log "  s${p}.${r}  →  ${host} (api=${NODE_ADDRS[$node_idx]}:$((API_BASE + idx)))"
    ssh_cmd "$host" \
        "OVERRIDE_RF='${RF}' bash ${MADKV}/scripts/p3/start_server.sh ${p} ${r} ${MADKV}/scripts/p3/config.sh" &
  done
done
wait

# ── Wait for cluster to become ready ─────────────────────────────────────────
log "Waiting for all servers to register with manager..."
total=$((NPARTS * RF))
for ((attempt=1; attempt<=60; attempt++)); do
  ready=$(ssh_cmd "${NODE_HOSTS[1]}" \
    "grep -c 'cluster ready state: ready' /tmp/madkv-p3/logs/manager-0.log 2>/dev/null || true")
  if [[ "${ready:-0}" -ge 1 ]]; then
    log "Cluster READY after ${attempt}s  (managers=${MANAGERS}  servers=${SERVERS})"
    break
  fi
  if [[ $attempt -eq 60 ]]; then
    log "WARNING: cluster did not report ready after 60s — check logs on node1"
  fi
  sleep 1
done

log "═══════════════════════════════════════════════════════════"
log "Cluster started.  Topology:"
log "  Partitions : ${NPARTS}"
log "  RF         : ${RF}"
log "  MANAGERS   : ${MANAGERS}"
log "  SERVERS    : ${SERVERS}"
log ""
log "To run a client:"
log "  ${MADKV}/kvstore/bin/kvclient --manager_addrs ${MANAGERS}"
log ""
log "To run the fuzzer:"
log "  just p3::fuzz server_rf=${RF} managers=\"${MANAGERS}\""
log ""
log "To stop everything:"
log "  ./scripts/p3/stop_all.sh"
log "═══════════════════════════════════════════════════════════"
