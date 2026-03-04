#!/bin/bash
# Run all Project 1 tests on a remote server
# Usage: ./run_all_tests.sh <server_ip:port>

if [ $# -ne 1 ]; then
    echo "Usage: $0 <server_ip:port>"
    echo "Example: $0 10.10.1.2:3777"
    exit 1
fi

SERVER=$1
echo "======================================"
echo "Running all Project 1 tests"
echo "Server: $SERVER"
echo "======================================"
echo

# Test connectivity
echo "Testing connectivity..."
SERVER_IP=$(echo $SERVER | cut -d: -f1)
if ping -c 1 $SERVER_IP > /dev/null 2>&1; then
    echo "✓ Server is reachable"
else
    echo "✗ Cannot reach server at $SERVER_IP"
    exit 1
fi
echo

# Run testcases
echo "======================================"
echo "Running testcases..."
echo "======================================"
for i in 1 2 3 4 5; do
    echo "--- Test $i ---"
    just p1::testcase $i $SERVER
    echo
done

# Run fuzz tests
echo "======================================"
echo "Running fuzz tests..."
echo "======================================"
echo "--- Single client, no conflicts ---"
just p1::fuzz 1 no $SERVER
echo

echo "--- 3 clients, no conflicts ---"
just p1::fuzz 3 no $SERVER
echo

echo "--- 3 clients, with conflicts ---"
just p1::fuzz 3 yes $SERVER
echo

# Run benchmarks
echo "======================================"
echo "Running YCSB benchmarks..."
echo "======================================"
echo "--- Single client, workloads A-F ---"
for wl in a b c d e f; do
    echo "Workload $wl..."
    just p1::bench 1 $wl $SERVER
    sleep 2
done
echo

echo "--- Multi-client scaling (A, C, E) ---"
for ncli in 10 25 40 55 70 85; do
    echo "$ncli clients..."
    just p1::bench $ncli a $SERVER
    just p1::bench $ncli c $SERVER
    just p1::bench $ncli e $SERVER
    sleep 2
done

# Generate report
echo "======================================"
echo "Generating report..."
echo "======================================"
just p1::report

echo
echo "======================================"
echo "All tests complete!"
echo "Results in /tmp/madkv-p1/"
echo "Report in report/proj1.md"
echo "======================================"
