set working-directory := '..'

# list project 2 just recipes
default:
    @just --list p2 --unsorted

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

# run your KV store manager
manager man_port="3666" servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779":
    ./kvstore/bin/kvmanager "0.0.0.0:{{man_port}}" "{{servers}}"

# run your KV store server
server id="0" manager="127.0.0.1:3666" api_port="3777" backer_path="./backer.s0":
    mkdir -p "{{backer_path}}"
    ./kvstore/bin/kvserver "{{id}}" "{{manager}}" "{{api_port}}" "{{backer_path}}"

# run you KV store client in stdin/out interface mode
client manager="127.0.0.1:3666":
    ./kvstore/bin/kvclient --manager "{{manager}}"

# kill all processes of your KV store system
kill:
    -pkill -9 kvmanager > /dev/null 2>&1
    -pkill -9 kvserver > /dev/null 2>&1
    -pkill -9 kvclient > /dev/null 2>&1

# NOTE: feel free to add more recipes as you see fit...
#       also feel free to add extra parameters to the recipes as you see fit,
#       but don't change the existing parameters

python_run := if `which uv || true` != "" { "uv run" } else { "python3" }
tmpdir_prefix := "/tmp/madkv-p2"

# launch the KV service components on node (see README for conventions)
service node="s0" manager="127.0.0.1:3666" servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779" backer_prefix="./backer":
    just p2::build
    just utils::build
    cargo run -p runner -r --bin service -- \
        --server-just-args p2::server \
            $({{python_run}} justmod/xtract.py partid "{{node}}") \
            "{{manager}}" \
            $({{python_run}} justmod/xtract.py portof "{{servers}}" "{{node}}") \
            "{{backer_prefix}}.{{node}}" \
        --manager-just-args p2::manager \
            $({{python_run}} justmod/xtract.py portof "{{manager}}" "{{node}}") \
            "{{servers}}" \
        --node-id "{{node}}"

# ensure a subdir under 'tmp/' exists
tmpdir subdir:
    mkdir -p "{{tmpdir_prefix}}/{{subdir}}"

# run a fuzz testing scenario
fuzz nservers="1" crashing="no" manager="127.0.0.1:3666": (tmpdir "fuzz")
    just p2::build
    just utils::build
    cargo run -p runner -r --bin fuzzer -- \
        --num-clis 5 \
        --conflict \
        --client-just-args p2::client "{{manager}}" \
        | tee "{{tmpdir_prefix}}/fuzz/fuzz-{{nservers}}-{{crashing}}.log"
    just p2::kill

# run a YCSB benchmark workload
bench nclis wload nservers="1" manager="127.0.0.1:3666": (tmpdir "bench")
    just p2::build
    just utils::build
    just utils::ycsb
    cargo run -p runner -r --bin bencher -- \
        --num-clis "{{nclis}}" \
        --workload "{{wload}}" \
        --client-just-args p2::client "{{manager}}" \
        | tee "{{tmpdir_prefix}}/bench/bench-{{nclis}}-{{wload}}-{{nservers}}.log"
    just p2::kill

# generate .md report template from existing results (wip)
report:
    {{python_run}} sumgen/proj2.py
