# CS 739 MadKV Project 3

**Group members**: Karthik Reddy Jannupalli `jannupalli@wisc.edu`, Shivam Mittal `smittal39@wisc.edu`

---

## Design Walkthrough

### Architecture Overview

MadKV P3 adds **Raft-based replication** on top of the P2 key-value server, providing fault tolerance against up to ⌊(RF−1)/2⌋ simultaneous server crashes while maintaining linearizable reads and writes. P3 also includes the **Replicated Manager bonus (+15%)**, making the cluster metadata service itself fault-tolerant via Raft consensus among 3 manager replicas.

The system has two components:

- **Manager (`kvmanager`)**: In P3 base, a single-node metadata server. With the bonus, 3 replicas run a Raft group so the manager itself can survive up to 1 crash. It accepts `RegisterServer` RPCs from replica nodes and serves `GetCluster` RPCs to clients so they can discover partition leaders. Non-leader manager replicas return `ready=false`, causing clients to retry against other manager addresses until they reach the Raft leader.
- **KV Server (`kvserver`)**: Each of the `NPARTS=2` partitions is replicated across `RF=5` replicas. Every replica embeds a `RaftNode` instance that handles consensus. Clients send Put/Get/Swap/Delete/Scan to any server; non-leaders transparently return a `NOT_LEADER:<addr>` hint so the client retries against the true leader.

### Raft Implementation (`raft.h`)

The Raft layer is implemented as a single-header C++ class `RaftNode` with the following design:

**State machine and threads**:
- All Raft state (current term, voted-for, log, commit index, next/match indices) is protected by a single mutex `mu_`.
- Three background threads drive the protocol: `ElectionLoop` (monitors heartbeat timeout, starts elections), `ReplicationLoop` (leader sends `AppendEntries` and heartbeats to all peers), and `ApplyLoop` (drains committed entries to the KV state machine via a caller-supplied `apply_fn`).
- Randomised election timeouts (2–4 seconds) prevent split votes.

**Write path**:
1. Client calls `Put` → `server.cpp` → `RaftNode::Submit(KVCmd)`.
2. `Submit` appends the command to the in-memory log, calls `fdatasync` to persist it, increments the leader's `matchIndex`, calls `AdvanceCommitIndex()`, signals the replication thread, and then blocks on a `std::promise<KVResult>` future.
3. The replication thread sends the entry to all peers; once a quorum (majority) confirms, `AdvanceCommitIndex` fires and the apply thread calls the `apply_fn` which resolves the promise and returns to the client.

**Read path**:
- Reads are served directly from the leader's in-memory `std::map<string,string>` without going through the log, providing low-latency reads.

**Persistence**:
- Durable state is stored in two files under `backer_path/`: `raft_meta` (current term + voted-for) and `raft_log` (append-only log entries). Both are fsynced after writes using `fdatasync`, ensuring durability across crashes.
- On restart, the node replays all committed log entries (up to `commit_index`) to rebuild in-memory state before joining the cluster, ensuring consistent state recovery.

**Leader election and recovery**:
- On follower timeout, the node increments its term, votes for itself, and sends `RequestVote` RPCs to all peers. A candidate wins if it receives votes from a majority while its log is at least as up-to-date as voters' logs.
- After a crash and restart, a server reads its `raft_meta` and `raft_log` from disk, replays committed entries, rejoins the cluster, and receives any missing log entries from the current leader via normal `AppendEntries` RPCs (log catch-up).

### Replicated Manager (Bonus: +15%)

The `kvmanager` was extended to run as a 3-replica Raft group (`MGR_RF=3`), co-located on one physical node using different ports (API ports 3666–3668, P2P ports 3606–3608). Key design decisions:

- **`RaftManagerService`**: A new `ClusterManager::Service` implementation that embeds a `RaftNode`. `RegisterServer` RPCs are submitted as `PUT(server_id, api_addr)` commands through Raft (write-through-consensus). `GetCluster` is served only by the Raft leader; non-leaders return `ready=false`.
- **`ManagerRaftServiceImpl`**: A separate gRPC service on the P2P port that forwards `RequestVote` and `AppendEntries` RPCs to the embedded `RaftNode`.
- **`ApplyCommand` callback**: Committed `PUT` entries mark the corresponding server slot as registered in the in-memory `registered_` table. This ensures all manager replicas converge to identical registration state.
- **Client transparency**: The `kvclient` already round-robins across all manager addresses. Since non-leaders return `ready=false`, clients naturally discover the leader without any protocol changes.
- **Recovery**: When a new leader is elected after a manager crash, it replays all committed registrations from its Raft log before serving requests, ensuring full cluster state is visible immediately.

### Client Protocol

Clients contact the manager to discover the cluster (partitions and per-partition server lists), then send KV operations directly to the partition leader. If a leader responds with `NOT_LEADER`, the client retries against the indicated address. If the leader crashes mid-operation, the client retries with exponential backoff until a new leader is elected (typically within 2–4 s). For the replicated manager, the client retries across multiple manager addresses until it finds the Raft leader.

### Partition Mapping

Keys are hashed by `std::hash<std::string>` modulo `NPARTS` to pick a partition. Each partition has its own Raft group of `RF` replicas, so the system can sustain ⌊(RF−1)/2⌋ crashes per partition independently.

---

## Self-provided Testcases

### Explanations

The nine testcase scenarios are implemented in `scripts/p3/cloudlab_test.sh` and cover correctness, fault-tolerance, persistence, and the bonus replicated manager. The suite achieves **22/22 checks passing** on a live CloudLab cluster (NPARTS=2, RF=5, MGR_RF=3).

**Test 1 — Basic PUT / GET / SWAP / DELETE / SCAN** (8 checks):  
Starts a fresh RF=5 cluster, issues all supported KV operations, and verifies each response. Confirms the complete end-to-end write path (client → Raft leader → quorum → apply → response) and that SCAN returns the correct ordered range.

**Test 2 — Multi-partition routing** (1 check):  
Writes 20 keys designed to spread across both partitions (hash-based routing) and verifies all 20 reads return correct values. Confirms that the client correctly routes each key to its partition's leader.

**Test 3 — DEMO 1: 2 follower failures per partition (RF=5, f=2)** (3 checks):  
Kills the 2 non-leader replicas in each partition (leaving 3/5 alive, still a quorum), then verifies reads and writes still succeed. Tests that majority fault-tolerance (⌊(5−1)/2⌋ = 2) works end-to-end.

**Test 4 — DEMO 2: Leader failure + Raft election + client recovery** (2 checks):  
Writes data, identifies and kills the Raft leader of partition 0, waits 6 s for election, then verifies that (a) pre-crash data is still readable and (b) new writes succeed. Confirms leader completeness (no committed write is lost) and client leader-retry logic.

**Test 5 — DEMO 4: Quorum loss → partition stalls, other partition unaffected** (1 check):  
Kills 3 of 5 replicas in partition 0 (exceeding f=2), then verifies that requests to the dead partition time out while partition 1 continues to serve. Confirms partition independence and correct Raft behaviour under quorum loss.

**Test 6 — DEMO 3: Manager crash + full cluster restart + data persistence** (2 checks):  
Writes data, kills the manager, restarts the full cluster (without `--clean`), and verifies that written keys are still readable after restart. Tests that both Raft log persistence and manager crash-recovery work correctly together.

**Test 7 — Full cluster restart with data persistence** (2 checks):  
Writes data, issues a controlled full cluster stop, restarts without wiping backer directories, and verifies all keys survive. This specifically validates that `raft_log` and `raft_meta` files are correctly reloaded and replayed on restart.

**Test 8 — Concurrent clients** (1 check):  
Launches 5 parallel `kvclient` processes that each write 2 unique keys simultaneously, then reads back all 10 keys serially and verifies correctness. Confirms that concurrent writes through the single Raft leader serialise correctly without losing data.

**Test 9 (BONUS) — Replicated Manager: leader crash, service continues** (2 checks):  
Writes baseline data, identifies the manager Raft leader (via log inspection), kills only that specific manager replica (by matching its unique `--man_port` in the process arguments), waits 6 s for a new election among the remaining 2 replicas, then verifies that a fresh client can both read existing data and write new keys. Confirms that the manager Raft group achieves fault-tolerance and that the new leader correctly serves the full registration state.

### Summary

| Test | Scenario | Checks | Result |
|:---:|:---|:---:|:---:|
| 1 | Basic PUT/GET/SWAP/DELETE/SCAN | 8 | ✓ PASS |
| 2 | Multi-partition routing (20 keys) | 1 | ✓ PASS |
| 3 | DEMO 1 — 2 follower failures per partition | 3 | ✓ PASS |
| 4 | DEMO 2 — Leader failure + re-election | 2 | ✓ PASS |
| 5 | DEMO 4 — Quorum loss → stall | 1 | ✓ PASS |
| 6 | DEMO 3 — Manager crash + restart | 2 | ✓ PASS |
| 7 | Full cluster restart — data persisted | 2 | ✓ PASS |
| 8 | Concurrent clients (5 parallel) | 1 | ✓ PASS |
| 9 (bonus) | Replicated Manager — leader crash | 2 | ✓ PASS |
| **Total** | | **22** | **22/22 PASS** |

---

## Fuzz Testing

server_rf | crashing | outcome
:-: | :-: | :-:
5 | no | PASSED
5 | yes | PASSED


### Comments

Both fuzz configurations with RF=5 passed linearizability verification.

**No-crash fuzz (RF=5)**: 5 concurrent clients each issuing 1000 randomised Put/Swap/Get/Delete operations (5000 total). The fuzzer records a per-key history of all submitted operations and observed responses, then checks whether there exists a legal sequential interleaving (linearisation) consistent with real-time ordering. All 5000 operations passed — the Raft leader serialises every write through the consensus log, so the effective execution order is exactly the order entries are committed, which is always linearisable.

**Crash fuzz (RF=5)**: Same 5-client workload, but 60 s into the run the leader of partition 0 was killed (forcing a re-election) and then a follower was also killed, leaving 3 of 5 replicas alive (still a quorum). Clients briefly stall while a new leader is elected (~2–4 s), then continue. The fuzzer tolerates these gaps and verifies linearisability over the complete history including the crash window. The result was PASSED, confirming that:
1. No committed write was lost across the leader failover.
2. Operations that returned errors during the crash window were correctly excluded from the linearisability check.
3. The new leader's state was fully consistent with all entries the old leader had committed.

---

## YCSB Benchmarking

<u>10 clients throughput/latency across workloads & replication factors:</u>

![ten-clients](plots-p3/ycsb-ten-clients.png)

<u>Agg. throughput trend vs. number of clients with different replication factors:</u>

![tput-trend](plots-p3/ycsb-tput-trend.png)

### Comments

#### Per-workload results (10 clients)

| Workload | Mix | RF=1 | RF=3 | RF=5 |
|:---:|:---:|:---:|:---:|:---:|
| A | 50% read / 50% update | 68.9 ops/s | 31.7 ops/s | 32.2 ops/s |
| B | 95% read / 5% update  | 311.9 ops/s | 185.1 ops/s | 205.2 ops/s |
| C | 100% read              | 509.1 ops/s | 495.0 ops/s | 499.9 ops/s |
| D | 95% read / 5% insert  | 308.3 ops/s | 175.4 ops/s | 190.0 ops/s |
| E | 95% scan / 5% insert  | 205.0 ops/s | 145.7 ops/s | 145.3 ops/s |
| F | 50% read / 50% RMW    | 100.0 ops/s | 36.1 ops/s | 37.3 ops/s |

**Key observations**:

1. **Read-only workload (C) is almost RF-agnostic**: Throughput (~500 ops/s) is nearly identical across RF=1, 3, and 5 because reads are served directly from the leader's in-memory map without touching the Raft log. Replication factor adds no overhead for reads.

2. **Write-heavy workloads (A, F) are severely penalised by RF>1**: Workload A drops from 68.9 to 31.7 ops/s going from RF=1 to RF=3 (a 2.2× slowdown). Workload F (read-modify-write) drops from 100.0 to 36.1 ops/s. The bottleneck is `fdatasync` — every Raft log append and metadata save calls `fdatasync`, and on CloudLab spinning disks these calls each take ~23 ms. With RF=3, the leader must wait for 2 of 3 followers to acknowledge before committing, serialising disk I/O across the replication path.

3. **RF=3 and RF=5 perform similarly**: Both have similar throughput for write-heavy workloads (within ~5%) because the quorum size increases from 2 (RF=3) to 3 (RF=5), but the critical-path bottleneck is still the leader's own `fdatasync` latency, not the additional follower.

4. **Workload F (RMW) is the worst for high RF**: Each read-modify-write involves one read (fast) plus one write through Raft (slow), with no pipelining. At RF=5 it achieves only 37.3 ops/s.

#### Client scaling results (YCSB-A, RF=1 vs RF=5)

| #clients | RF=1 | RF=5 |
|:---:|:---:|:---:|
| 1  | 42.4 ops/s | 4.4 ops/s |
| 10 | 68.9 ops/s | 32.2 ops/s |
| 20 | 69.5 ops/s | 52.7 ops/s |
| 30 | 64.7 ops/s | 56.6 ops/s |

**Key observations**:

1. **RF=1 saturates early (~10 clients)**: With a single replica, the leader's disk throughput is the bottleneck. Adding more clients beyond ~10 does not increase total throughput and can slightly decrease it due to lock contention in the mutex-protected Raft state.

2. **RF=5 scales more gradually**: At 1 client, RF=5 is much slower (4.4 vs 42.4 ops/s) because each write requires 3 followers to ack, amplifying latency. But as client count grows, request pipelining across the log hides some of the replication latency, and RF=5 throughput catches up partially (56.6 ops/s at 30 clients vs 64.7 for RF=1).

3. **Neither configuration scales linearly**: The single-leader Raft architecture fundamentally serialises all writes through one node. True write scalability would require partitioning across more Raft groups or using a leaderless protocol.

---

## Bonus Work

### Replicated Manager (+15%)

The `kvmanager` was made fault-tolerant using the same Raft consensus layer as the KV servers. Three manager replicas (`MGR_RF=3`) run on the same physical node using distinct ports (to work around `libgrpc++` unavailability on additional CloudLab nodes). The implementation is in `manager.cpp` as `RaftManagerService` and `ManagerRaftServiceImpl`.

**What was implemented**:
- `RegisterServer` is a write operation committed through Raft; only the Raft leader accepts these RPCs.
- `GetCluster` is a read served from the leader's in-memory state; non-leaders return `ready=false`, causing clients to naturally retry other addresses.
- Server registrations are encoded as Raft log entries (`PUT key=server_id val=api_addr`) and applied to the in-memory `registered_[]` table via a callback. This ensures all replicas converge to identical state after any number of leader changes.
- On restart, committed Raft log entries are replayed before the network starts, so the new leader immediately has full registration state.

**Recovery demonstration (Test 9)**:
The test writes data, kills the manager Raft leader, waits 6 s for the remaining 2 replicas to elect a new leader, then issues both a read and a write from a fresh client — both succeed. This confirms:
1. The new manager leader correctly replays the registration log and serves cluster topology.
2. KV servers continue operating without interruption (they already connected to their Raft groups at startup).
3. New clients can discover the cluster and submit KV operations through the new manager leader.

**Scripts supporting replicated manager**:
- `scripts/p3/start_manager_replica.sh`: Starts one manager replica given a replica ID; computes ports and peer addresses from `config.sh`.
- `scripts/p3/start_all.sh`: Loops over `MGR_RF` replicas, starts each in parallel, waits 3 s for initial Raft election before proceeding.
- `scripts/p3/stop_all.sh`: De-duplicates the manager host list (all 3 replicas share a host) and issues a single kill per unique host.

---

## Additional Discussion

### Performance Bottleneck: `fdatasync`

The primary performance bottleneck identified in this implementation is synchronous disk I/O in the Raft persistence path. Each `Put` or `Swap` call triggers two `fdatasync` calls: one in `AppendLogEntryLocked` (to persist the new log entry) and one in `SaveMeta` (to persist the updated term and voted-for). On CloudLab nodes with spinning HDDs, each `fdatasync` takes approximately 20–25 ms. This limits write throughput to roughly 20–40 ops/s per leader regardless of replication factor.

**Potential optimisations** (not implemented, for discussion):
- **Batching writes**: Accumulate multiple log entries and fsync once, amortising disk latency across concurrent clients. This is the approach used by production systems like etcd.
- **Using `tmpfs` for the log**: Storing `raft_log` on a RAM-backed filesystem would eliminate disk latency entirely, at the cost of durability across power failures (acceptable if all replicas must crash simultaneously to lose data).
- **Async fsync with barriers**: Allow the apply thread to advance the commit index speculatively and use a barrier before responding to clients, overlapping disk I/O with network round-trips.

### Linearisability via Raft

Our implementation achieves linearisability because:
1. All writes are serialised through the Raft log. The commit order defines the linearisation point.
2. Reads are served by the leader from its authoritative in-memory state. Since only the leader can serve reads (non-leaders return `NOT_LEADER`), and the leader only applies entries after a quorum commits them, the in-memory state always reflects all committed writes at the time of the read.
3. During leader transitions, clients retry until they find the new leader. The new leader's log contains all entries committed by the old leader (guaranteed by Raft's leader completeness property), so no committed write is ever invisible to a subsequent read.

### Process Isolation Bug and Fix

During CloudLab testing we discovered that using `pkill -9 -f kvserver` in remote SSH commands was silently killing the **remote shell itself** — because the shell process's own command-line argument (the `bash -c "..."` string) contained the word `kvserver`, which `pkill -f` matched. This caused cleanup to abort mid-execution, leaving 8–12 stale `kvserver` processes on each node, which then competed for Raft P2P ports and caused election storms across all test runs.

The fix was to use `pkill -9 kvserver` (without `-f`) in all remote commands, which matches only the process name (binary basename) rather than the full command line, avoiding the self-kill race condition.
