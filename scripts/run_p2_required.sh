#!/bin/bash

set -e

MANAGER_ADDR="10.10.1.2:3666"

prompt_restart_servers() {
    local nservers="$1"
    local desc="$2"
    echo ""
    echo ">>> Next case: ${desc}"
    echo ">>> Requires ${nservers} partition server(s) with fresh storage."
    echo ">>> Please start your service on all nodes, then press Enter."
    echo ">>>   just p2::service <node_id> <manager> <servers> <backer_prefix>"
    read -r
}

echo "============================================"
echo " Starting Project 2 Automated Test Suite "
echo "============================================"

just p2::clean
just p2::build

echo ""
echo "--------------------------------------------"
echo " 1. Running Fuzz Tests"
echo "--------------------------------------------"

prompt_restart_servers 3 "Fuzz: 3 partitions, no crashing"
just p2::fuzz 3 no $MANAGER_ADDR

prompt_restart_servers 3 "Fuzz: 3 partitions, crashing"
just p2::fuzz 3 yes $MANAGER_ADDR

prompt_restart_servers 5 "Fuzz: 5 partitions, crashing"
just p2::fuzz 5 yes $MANAGER_ADDR

echo ""
echo "--------------------------------------------"
echo " 2. Running YCSB Benchmarks"
echo "--------------------------------------------"

WORKLOADS=("a" "b" "c" "d" "e" "f")
PARTITIONS=(1 3 5)

for p in "${PARTITIONS[@]}"; do
    for w in "${WORKLOADS[@]}"; do
        prompt_restart_servers "$p" "Bench: 10 clients | Workload $w | $p partitions"
        just p2::bench 10 "$w" "$p" $MANAGER_ADDR
    done
done

CLIENT_COUNTS=(1 10 20 30)
SCALING_PARTITIONS=(1 5)

for p in "${SCALING_PARTITIONS[@]}"; do
    for c in "${CLIENT_COUNTS[@]}"; do
        if [ "$c" -eq 10 ]; then
            continue
        fi
        prompt_restart_servers "$p" "Bench: $c clients | Workload a | $p partitions"
        just p2::bench "$c" "a" "$p" $MANAGER_ADDR
    done
done

echo ""
echo "--------------------------------------------"
echo " 3. Generating Report"
echo "--------------------------------------------"
echo "Running report generation script..."
just p2::report

echo "============================================"
echo " All tasks completed successfully! "
echo " Please check your generated Markdown report and plot images."
echo "============================================"
