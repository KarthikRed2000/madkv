set working-directory := '..'

# list project 3 just recipes
default:
    @just --list p3 --unsorted

# install extra dependencies of your KV code
deps:
    #!/usr/bin/env bash
    sudo apt update
    sudo apt install -y build-essential cmake pkg-config
    sudo apt install -y libgrpc++-dev libprotobuf-dev protobuf-compiler-grpc
    sudo apt install -y librocksdb-dev default-jre

# build your executables in release mode
build:
    #!/usr/bin/env bash
    set -euo pipefail
    cd kvstore
    mkdir -p build
    cd build
    cmake -DCMAKE_BUILD_TYPE=Release ..
    jobs=$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)
    make -j"${jobs}"
    cmake --install .

# clean the build of your executables
clean:
    rm -rf kvstore/build kvstore/bin/kvserver kvstore/bin/kvclient kvstore/bin/kvmanager

# run your KV store manager replica
manager rep_id="0" \
        man_port="3666" \
        p2p_port="3606" \
        peers="127.0.0.1:3607,127.0.0.1:3608" \
        server_rf="3" \
        servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779,127.0.0.1:3780,127.0.0.1:3781,127.0.0.1:3782"\
        backer_path="./backer.m.0":
    ./kvstore/bin/kvmanager \
        --replica_id {{rep_id}} \
        --man_port {{man_port}} \
        --p2p_port {{p2p_port}} \
        --peer_addrs {{peers}} \
        --server_rf {{server_rf}} \
        --server_addrs {{servers}} \
        --backer_path {{backer_path}}

# run your KV store server replica
server part_id="0" \
       rep_id="0" \
       managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" \
       api_port="3777" \
       p2p_port="3707" \
       peers="127.0.0.1:3708,127.0.0.1:3709" \
       backer_path="./backer.s0.0":
    mkdir -p "{{backer_path}}"
    ./kvstore/bin/kvserver \
        --partition_id {{part_id}} \
        --replica_id {{rep_id}} \
        --manager_addrs {{managers}} \
        --api_port {{api_port}} \
        --p2p_port {{p2p_port}} \
        --peer_addrs {{peers}} \
        --backer_path {{backer_path}}

# run you KV store client in stdin/out interface mode
client managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668":
    ./kvstore/bin/kvclient --manager_addrs {{managers}}

# kill all processes of your KV store system
kill:
    -pkill -9 kvmanager > /dev/null 2>&1
    -pkill -9 kvserver > /dev/null 2>&1
    -pkill -9 kvclient > /dev/null 2>&1

# NOTE: feel free to add more recipes as you see fit...
#       also feel free to add extra parameters to the recipes as you see fit,
#       but don't change the existing parameters

python_run := if `which uv || true` != "" { "uv run" } else { "python3" }
tmpdir_prefix := "/tmp/madkv-p3"

# launch the KV service components on node (see README for conventions)
service node="s0.0" \
        managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" \
        manager_p2ps="127.0.0.1:3606,127.0.0.1:3607,127.0.0.1:3608" \
        server_rf="3" \
        servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779,127.0.0.1:3780,127.0.0.1:3781,127.0.0.1:3782" \
        server_p2ps="127.0.0.1:3707,127.0.0.1:3708,127.0.0.1:3709,127.0.0.1:3710,127.0.0.1:3711,127.0.0.1:3712" \
        backer_prefix="./backer":
    just p3::build
    just utils::build
    cargo run -p runner -r --bin service -- \
        --server-just-args p3::server \
            $({{python_run}} justmod/xtract.py partid "{{node}}") \
            $({{python_run}} justmod/xtract.py repid "{{node}}") \
            "{{managers}}" \
            $({{python_run}} justmod/xtract.py portof "{{servers}}" "{{node}}" "{{server_rf}}") \
            $({{python_run}} justmod/xtract.py portof "{{server_p2ps}}" "{{node}}" "{{server_rf}}") \
            $({{python_run}} justmod/xtract.py peersof "{{server_p2ps}}" "{{node}}" "{{server_rf}}") \
            "{{backer_prefix}}.{{node}}" \
        --manager-just-args p3::manager \
            $({{python_run}} justmod/xtract.py repid "{{node}}") \
            $({{python_run}} justmod/xtract.py portof "{{managers}}" "{{node}}") \
            $({{python_run}} justmod/xtract.py portof "{{manager_p2ps}}" "{{node}}") \
            $({{python_run}} justmod/xtract.py peersof "{{manager_p2ps}}" "{{node}}") \
            "{{server_rf}}" \
            "{{servers}}" \
            "{{backer_prefix}}.{{node}}" \
        --node-id "{{node}}"

# ensure a subdir under 'tmp/' exists
tmpdir subdir:
    mkdir -p "{{tmpdir_prefix}}/{{subdir}}"

# run a fuzz testing scenario
fuzz server_rf="5" crashing="no" \
     managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668": (tmpdir "fuzz")
    just p3::build
    just utils::build
    cargo run -p runner -r --bin fuzzer -- \
        --num-clis 5 \
        --conflict \
        --client-just-args p3::client "{{managers}}" \
        | tee "{{tmpdir_prefix}}/fuzz/fuzz-{{server_rf}}-{{crashing}}.log"
    just p3::kill

# run a YCSB benchmark workload
bench nclis wload server_rf="3" \
      managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668": (tmpdir "bench")
    just p3::build
    just utils::build
    just utils::ycsb
    cargo run -p runner -r --bin bencher -- \
        --num-clis "{{nclis}}" \
        --workload "{{wload}}" \
        --client-just-args p3::client "{{managers}}" \
        | tee "{{tmpdir_prefix}}/bench/bench-{{nclis}}-{{wload}}-{{server_rf}}.log"
    # just p3::kill

# generate .md report template from existing results (wip)
report:
    {{python_run}} sumgen/proj3.py
