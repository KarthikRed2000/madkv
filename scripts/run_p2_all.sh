#!/usr/bin/env bash
set -euo pipefail

NODE0_IP="10.10.1.1"
NODE1_IP="10.10.1.2"
MANAGER="${NODE1_IP}:3666"
MADKV="$HOME/madkv"
BACKER="/tmp/madkv-p2-backer/backer"
PLOG="/tmp/madkv-p2-progress.log"

export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"

log() { echo "[$(date '+%H:%M:%S')] $1" | tee -a "$PLOG"; }

remote() {
    ssh -o StrictHostKeyChecking=no "$NODE1_IP" \
        "export PATH=\$HOME/.cargo/bin:\$PATH; $1"
}

get_servers() {
    case "$1" in
        1) echo "${NODE1_IP}:3777" ;;
        3) echo "${NODE1_IP}:3777,${NODE1_IP}:3778,${NODE1_IP}:3779" ;;
        5) echo "${NODE1_IP}:3777,${NODE1_IP}:3778,${NODE1_IP}:3779,${NODE1_IP}:3780,${NODE1_IP}:3781" ;;
    esac
}

kill_cluster() {
    just p2::kill 2>/dev/null || true
    remote "cd $MADKV && just p2::kill" 2>/dev/null || true
    sleep 1
}

start_cluster() {
    local np="$1"
    local svrs
    svrs="$(get_servers "$np")"
    kill_cluster
    log "Starting ${np}-partition cluster (node1=server)"
    remote "cd $MADKV && rm -rf /tmp/madkv-p2-backer && mkdir -p /tmp/madkv-p2-backer"
    remote "nohup $MADKV/kvstore/bin/kvmanager 0.0.0.0:3666 '$svrs' >/tmp/madkv-p2-backer/mgr.log 2>&1 & disown; sleep 1"
    case "$np" in
        1)
            remote "nohup $MADKV/kvstore/bin/kvserver 0 $MANAGER 3777 ${BACKER}.s0 >/tmp/madkv-p2-backer/s0.log 2>&1 & disown; sleep 1"
            ;;
        3)
            remote "nohup $MADKV/kvstore/bin/kvserver 0 $MANAGER 3777 ${BACKER}.s0 >/tmp/madkv-p2-backer/s0.log 2>&1 & nohup $MADKV/kvstore/bin/kvserver 1 $MANAGER 3778 ${BACKER}.s1 >/tmp/madkv-p2-backer/s1.log 2>&1 & nohup $MADKV/kvstore/bin/kvserver 2 $MANAGER 3779 ${BACKER}.s2 >/tmp/madkv-p2-backer/s2.log 2>&1 & disown -a; sleep 1"
            ;;
        5)
            remote "nohup $MADKV/kvstore/bin/kvserver 0 $MANAGER 3777 ${BACKER}.s0 >/tmp/madkv-p2-backer/s0.log 2>&1 & nohup $MADKV/kvstore/bin/kvserver 1 $MANAGER 3778 ${BACKER}.s1 >/tmp/madkv-p2-backer/s1.log 2>&1 & nohup $MADKV/kvstore/bin/kvserver 2 $MANAGER 3779 ${BACKER}.s2 >/tmp/madkv-p2-backer/s2.log 2>&1 & nohup $MADKV/kvstore/bin/kvserver 3 $MANAGER 3780 ${BACKER}.s3 >/tmp/madkv-p2-backer/s3.log 2>&1 & nohup $MADKV/kvstore/bin/kvserver 4 $MANAGER 3781 ${BACKER}.s4 >/tmp/madkv-p2-backer/s4.log 2>&1 & disown -a; sleep 1"
            ;;
    esac
    sleep 5
    log "Cluster ready"
}

run_fuzz() {
    local np="$1" cr="$2"
    log "FUZZ ${np} parts crashing=${cr}"
    start_cluster "$np"
    cd "$MADKV"
    mkdir -p /tmp/madkv-p2/fuzz
    local logf="/tmp/madkv-p2/fuzz/fuzz-${np}-${cr}.log"
    set +e
    cargo run -p runner -r --bin fuzzer -- \
        --num-clis 5 --conflict \
        --client-just-args p2::client "$MANAGER" \
        >"$logf" 2>&1
    local rc=$?
    set -e
    kill_cluster
    local res
    res="$(grep -o 'PASSED\|FAILED' "$logf" 2>/dev/null || echo "exit=$rc")"
    log "FUZZ ${np}-${cr} => $res"
}

run_bench() {
    local nc="$1" wl="$2" np="$3"
    log "BENCH ${nc}cli ${wl} ${np}parts"
    start_cluster "$np"
    cd "$MADKV"
    mkdir -p /tmp/madkv-p2/bench
    local logf="/tmp/madkv-p2/bench/bench-${nc}-${wl}-${np}.log"
    set +e
    cargo run -p runner -r --bin bencher -- \
        --num-clis "$nc" --workload "$wl" \
        --client-just-args p2::client "$MANAGER" \
        >"$logf" 2>&1
    local rc=$?
    set -e
    kill_cluster
    local tp
    tp="$(grep 'Throughput' "$logf" 2>/dev/null | tail -1 | awk '{print $(NF-1)}' || echo "exit=$rc")"
    log "BENCH ${nc}-${wl}-${np} => tput=$tp"
}

: > "$PLOG"
log "P2 Full Test Suite - node0=$NODE0_IP node1=$NODE1_IP"

rm -rf /tmp/madkv-p2/fuzz /tmp/madkv-p2/bench
mkdir -p /tmp/madkv-p2/fuzz /tmp/madkv-p2/bench

log "==== Running bench a======"
run_bench 10 a 1

log "=== FUZZ TESTS ==="
run_fuzz 3 no
run_fuzz 3 yes
run_fuzz 5 yes

log "=== BENCH: 10cli all workloads ==="
for p in 1 3 5; do
    for w in a b c d e f; do
        run_bench 10 "$w" "$p"
    done
done

log "=== BENCH: scaling clients ==="
for p in 1 5; do
    for c in 1 20 30; do
        run_bench "$c" a "$p"
    done
done

log "=== REPORT ==="
cd "$MADKV"
mkdir -p report/plots-p2
echo "" | python3 sumgen/proj2.py -i /tmp/madkv-p2 -o report 2>&1 || {
    log "Report issue, trying script..."
    ./scripts/generate_p2_report.sh /tmp/madkv-p2 report 2>&1 || true
}

log "ALL DONE - fuzz=$(ls /tmp/madkv-p2/fuzz/*.log 2>/dev/null | wc -l) bench=$(ls /tmp/madkv-p2/bench/*.log 2>/dev/null | wc -l)"
