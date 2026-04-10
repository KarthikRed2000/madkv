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

# ── SSH targets — use internal experiment hostnames (node0..node21)
#    These resolve via /etc/hosts on every experiment node (10.10.1.1..22).
#    From outside the experiment (e.g. your laptop) use pc297.emulab.net etc.
NODE_HOSTS=(
  "node0"   # node0  – orchestrator / client
  "node1"   # node1  – manager
  "node2"   # node2  – s0.0
  "node3"   # node3  – s0.1
  "node4"   # node4  – s0.2
  "node5"   # node5  – s0.3
  "node6"   # node6  – s0.4
  "node7"   # node7  – s1.0
  "node8"   # node8  – s1.1
  "node9"   # node9  – s1.2
  "node10"  # node10 – s1.3
  "node11"  # node11 – s1.4
  "node12"  # node12 – client 0
  "node13"  # node13 – client 1
  "node14"  # node14 – client 2
  "node15"  # node15 – client 3
  "node16"  # node16 – client 4
  "node17"  # node17 – client 5
  "node18"  # node18 – client 6
  "node19"  # node19 – client 7
  "node20"  # node20 – client 8
  "node21"  # node21 – client 9
)

# ── Network addresses for gRPC — internal experiment IPs (10.10.1.x)
NODE_ADDRS=(
  "10.10.1.1"   # node0  – orchestrator
  "10.10.1.2"   # node1  – manager
  "10.10.1.3"   # node2  – s0.0
  "10.10.1.4"   # node3  – s0.1
  "10.10.1.5"   # node4  – s0.2
  "10.10.1.6"   # node5  – s0.3
  "10.10.1.7"   # node6  – s0.4
  "10.10.1.8"   # node7  – s1.0
  "10.10.1.9"   # node8  – s1.1
  "10.10.1.10"  # node9  – s1.2
  "10.10.1.11"  # node10 – s1.3
  "10.10.1.12"  # node11 – s1.4
  "10.10.1.13"  # node12 – client 0
  "10.10.1.14"  # node13 – client 1
  "10.10.1.15"  # node14 – client 2
  "10.10.1.16"  # node15 – client 3
  "10.10.1.17"  # node16 – client 4
  "10.10.1.18"  # node17 – client 5
  "10.10.1.19"  # node18 – client 6
  "10.10.1.20"  # node19 – client 7
  "10.10.1.21"  # node20 – client 8
  "10.10.1.22"  # node21 – client 9
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
