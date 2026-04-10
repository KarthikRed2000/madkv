#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# P3 Cluster Configuration  —  22 Emulab nodes, NPARTS=2, RF=5
#
# Layout
#   node0  (pc297)          orchestrator / client
#   node1  (pc550)          manager  (m.0)
#   node2  (pc315)  s0.0   partition 0, replica 0
#   node3  (pc431)  s0.1   partition 0, replica 1
#   node4  (pc499)  s0.2   partition 0, replica 2
#   node5  (pc308)  s0.3   partition 0, replica 3
#   node6  (pc299)  s0.4   partition 0, replica 4
#   node7  (pc439)  s1.0   partition 1, replica 0
#   node8  (pc304)  s1.1   partition 1, replica 1
#   node9  (pc552)  s1.2   partition 1, replica 2
#   node10 (pc253)  s1.3   partition 1, replica 3
#   node11 (pc497)  s1.4   partition 1, replica 4
#   node12 (pc513)          YCSB/fuzz client 0
#   node13 (pc309)          YCSB/fuzz client 1
#   node14 (pc320)          YCSB/fuzz client 2
#   node15 (pc283)          YCSB/fuzz client 3
#   node16 (pc525)          YCSB/fuzz client 4
#   node17 (pc433)          YCSB/fuzz client 5
#   node18 (pc430)          YCSB/fuzz client 6
#   node19 (pc306)          YCSB/fuzz client 7
#   node20 (pc288)          YCSB/fuzz client 8
#   node21 (pc438)          YCSB/fuzz client 9
# ─────────────────────────────────────────────────────────────────────────────

CLOUDLAB_USER="jann2000"

# ── SSH targets (user@hostname) ───────────────────────────────────────────────
NODE_HOSTS=(
  "${CLOUDLAB_USER}@pc297.emulab.net"   # node0  – orchestrator / client
  "${CLOUDLAB_USER}@pc550.emulab.net"   # node1  – manager
  "${CLOUDLAB_USER}@pc315.emulab.net"   # node2  – s0.0
  "${CLOUDLAB_USER}@pc431.emulab.net"   # node3  – s0.1
  "${CLOUDLAB_USER}@pc499.emulab.net"   # node4  – s0.2
  "${CLOUDLAB_USER}@pc308.emulab.net"   # node5  – s0.3
  "${CLOUDLAB_USER}@pc299.emulab.net"   # node6  – s0.4
  "${CLOUDLAB_USER}@pc439.emulab.net"   # node7  – s1.0
  "${CLOUDLAB_USER}@pc304.emulab.net"   # node8  – s1.1
  "${CLOUDLAB_USER}@pc552.emulab.net"   # node9  – s1.2
  "${CLOUDLAB_USER}@pc253.emulab.net"   # node10 – s1.3
  "${CLOUDLAB_USER}@pc497.emulab.net"   # node11 – s1.4
  "${CLOUDLAB_USER}@pc513.emulab.net"   # node12 – client 0
  "${CLOUDLAB_USER}@pc309.emulab.net"   # node13 – client 1
  "${CLOUDLAB_USER}@pc320.emulab.net"   # node14 – client 2
  "${CLOUDLAB_USER}@pc283.emulab.net"   # node15 – client 3
  "${CLOUDLAB_USER}@pc525.emulab.net"   # node16 – client 4
  "${CLOUDLAB_USER}@pc433.emulab.net"   # node17 – client 5
  "${CLOUDLAB_USER}@pc430.emulab.net"   # node18 – client 6
  "${CLOUDLAB_USER}@pc306.emulab.net"   # node19 – client 7
  "${CLOUDLAB_USER}@pc288.emulab.net"   # node20 – client 8
  "${CLOUDLAB_USER}@pc438.emulab.net"   # node21 – client 9
)

# ── Network addresses used for gRPC (hostname without user@) ─────────────────
# Within an Emulab experiment these hostnames resolve correctly on all nodes.
# If you prefer private experiment IPs, run discover_ips.sh and paste them here.
NODE_ADDRS=(
  "pc297.emulab.net"   # node0  – orchestrator
  "pc550.emulab.net"   # node1  – manager
  "pc315.emulab.net"   # node2  – s0.0
  "pc431.emulab.net"   # node3  – s0.1
  "pc499.emulab.net"   # node4  – s0.2
  "pc308.emulab.net"   # node5  – s0.3
  "pc299.emulab.net"   # node6  – s0.4
  "pc439.emulab.net"   # node7  – s1.0
  "pc304.emulab.net"   # node8  – s1.1
  "pc552.emulab.net"   # node9  – s1.2
  "pc253.emulab.net"   # node10 – s1.3
  "pc497.emulab.net"   # node11 – s1.4
  "pc513.emulab.net"   # node12 – client 0
  "pc309.emulab.net"   # node13 – client 1
  "pc320.emulab.net"   # node14 – client 2
  "pc283.emulab.net"   # node15 – client 3
  "pc525.emulab.net"   # node16 – client 4
  "pc433.emulab.net"   # node17 – client 5
  "pc430.emulab.net"   # node18 – client 6
  "pc306.emulab.net"   # node19 – client 7
  "pc288.emulab.net"   # node20 – client 8
  "pc438.emulab.net"   # node21 – client 9
)

# Backward-compat alias used by older code that referenced NODE_IPS
NODE_IPS=("${NODE_ADDRS[@]}")

# Dedicated YCSB/fuzz client nodes (nodes 12–21)
CLIENT_HOSTS=(
  "${NODE_HOSTS[12]}"
  "${NODE_HOSTS[13]}"
  "${NODE_HOSTS[14]}"
  "${NODE_HOSTS[15]}"
  "${NODE_HOSTS[16]}"
  "${NODE_HOSTS[17]}"
  "${NODE_HOSTS[18]}"
  "${NODE_HOSTS[19]}"
  "${NODE_HOSTS[20]}"
  "${NODE_HOSTS[21]}"
)

# ── Topology ──────────────────────────────────────────────────────────────────
NPARTS=2    # number of key-space partitions
RF=5        # server replication factor (1, 3, or 5; must satisfy 2*RF+1 nodes per part)

# ── Port assignments ──────────────────────────────────────────────────────────
MAN_PORT=3666      # manager client-facing port
MGR_P2P_PORT=3606  # manager Raft P2P port (unused when single manager)
API_BASE=3777      # server API ports: 3777 + (part*RF + rep)
P2P_BASE=3707      # server Raft P2P ports: 3707 + (part*RF + rep)

# ── Paths (must be identical on every node) ───────────────────────────────────
MADKV="$HOME/madkv"
BACKER_PREFIX="/tmp/madkv-p3/backer"
LOG_DIR="/tmp/madkv-p3/logs"

# ─────────────────────────────────────────────────────────────────────────────
# Derived values — computed automatically; do not edit below this line
# ─────────────────────────────────────────────────────────────────────────────
MANAGER_ADDR="${NODE_ADDRS[1]}"
MANAGERS="${MANAGER_ADDR}:${MAN_PORT}"
MANAGER_P2PS="${MANAGER_ADDR}:${MGR_P2P_PORT}"

# SERVER_ADDRS[part*RF+rep] = hostname of that replica
declare -a SERVER_ADDRS
for ((i=0; i < NPARTS*RF; i++)); do
  SERVER_ADDRS[$i]="${NODE_ADDRS[$((i+2))]}"
done

# Comma-separated "addr:api_port" for all replicas (passed to manager --server_addrs)
SERVERS=""
SERVER_P2PS=""
for ((i=0; i < NPARTS*RF; i++)); do
  [[ -n "$SERVERS" ]]     && SERVERS+=","
  [[ -n "$SERVER_P2PS" ]] && SERVER_P2PS+=","
  SERVERS+="${SERVER_ADDRS[$i]}:$((API_BASE + i))"
  SERVER_P2PS+="${SERVER_ADDRS[$i]}:$((P2P_BASE + i))"
done

# ── Helpers ───────────────────────────────────────────────────────────────────

# Comma-separated P2P peer addresses for server (PART_ID, REP_ID), excluding self
get_server_peers() {
  local part_id="$1" rep_id="$2" peers=""
  for ((r=0; r<RF; r++)); do
    [[ $r -eq $rep_id ]] && continue
    local idx=$((part_id * RF + r))
    [[ -n "$peers" ]] && peers+=","
    peers+="${SERVER_ADDRS[$idx]}:$((P2P_BASE + idx))"
  done
  echo "${peers:-none}"
}

# API port for server (PART_ID, REP_ID)
get_server_api_port() { echo $((API_BASE + $1 * RF + $2)); }

# P2P port for server (PART_ID, REP_ID)
get_server_p2p_port() { echo $((P2P_BASE + $1 * RF + $2)); }

# Backer path for a node tag (e.g. "m.0", "s0.1")
get_backer() { echo "${BACKER_PREFIX}.${1}"; }
