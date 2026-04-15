cmake_minimum_required(VERSION 3.15)
project(madkv_kvstore)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find required packages
find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)

# Use pkg-config to find gRPC and Protobuf (works on Ubuntu 22.04)
pkg_check_modules(GRPC REQUIRED grpc++)
pkg_check_modules(PROTOBUF REQUIRED protobuf)
pkg_check_modules(ROCKSDB QUIET rocksdb)

if(ROCKSDB_FOUND)
    message(STATUS "Using RocksDB via pkg-config")
    set(ROCKSDB_INCLUDE_DIRS ${ROCKSDB_INCLUDE_DIRS})
    set(ROCKSDB_LIBRARY_DIRS ${ROCKSDB_LIBRARY_DIRS})
    set(ROCKSDB_LIBS ${ROCKSDB_LIBRARIES})
else()
    message(STATUS "rocksdb pkg-config not found, trying CMake fallback")
    find_path(ROCKSDB_INCLUDE_DIR rocksdb/db.h)
    find_library(ROCKSDB_LIBRARY NAMES rocksdb)
    if(NOT ROCKSDB_INCLUDE_DIR OR NOT ROCKSDB_LIBRARY)
        message(FATAL_ERROR
            "RocksDB not found. Install librocksdb-dev (Ubuntu) or rocksdb (Homebrew).")
    endif()
    set(ROCKSDB_INCLUDE_DIRS ${ROCKSDB_INCLUDE_DIR})
    set(ROCKSDB_LIBRARY_DIRS "")
    set(ROCKSDB_LIBS ${ROCKSDB_LIBRARY})
endif()

message(STATUS "Using gRPC ${GRPC_VERSION}")
message(STATUS "Using Protobuf ${PROTOBUF_VERSION}")

# Proto files
set(KVSTORE_PROTO_FILE ${CMAKE_CURRENT_SOURCE_DIR}/proto/kvstore.proto)
set(CLUSTER_PROTO_FILE ${CMAKE_CURRENT_SOURCE_DIR}/proto/cluster.proto)
set(RAFT_PROTO_FILE    ${CMAKE_CURRENT_SOURCE_DIR}/proto/raft.proto)

# Find protoc and grpc_cpp_plugin executables
find_program(PROTOC protoc REQUIRED)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

# ── kvstore proto ─────────────────────────────────────────────────────────────
set(KVSTORE_PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/kvstore.pb.cc")
set(KVSTORE_PROTO_HDRS "${CMAKE_CURRENT_BINARY_DIR}/kvstore.pb.h")
set(KVSTORE_GRPC_SRCS  "${CMAKE_CURRENT_BINARY_DIR}/kvstore.grpc.pb.cc")
set(KVSTORE_GRPC_HDRS  "${CMAKE_CURRENT_BINARY_DIR}/kvstore.grpc.pb.h")

add_custom_command(
    OUTPUT ${KVSTORE_PROTO_SRCS} ${KVSTORE_PROTO_HDRS} ${KVSTORE_GRPC_SRCS} ${KVSTORE_GRPC_HDRS}
    COMMAND ${PROTOC}
    ARGS --grpc_out="${CMAKE_CURRENT_BINARY_DIR}"
         --cpp_out="${CMAKE_CURRENT_BINARY_DIR}"
         -I="${CMAKE_CURRENT_SOURCE_DIR}/proto"
         --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
         ${KVSTORE_PROTO_FILE}
    DEPENDS ${KVSTORE_PROTO_FILE}
    COMMENT "Generating kvstore proto/grpc files"
)

# ── cluster proto ─────────────────────────────────────────────────────────────
set(CLUSTER_PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/cluster.pb.cc")
set(CLUSTER_PROTO_HDRS "${CMAKE_CURRENT_BINARY_DIR}/cluster.pb.h")
set(CLUSTER_GRPC_SRCS  "${CMAKE_CURRENT_BINARY_DIR}/cluster.grpc.pb.cc")
set(CLUSTER_GRPC_HDRS  "${CMAKE_CURRENT_BINARY_DIR}/cluster.grpc.pb.h")

add_custom_command(
    OUTPUT ${CLUSTER_PROTO_SRCS} ${CLUSTER_PROTO_HDRS} ${CLUSTER_GRPC_SRCS} ${CLUSTER_GRPC_HDRS}
    COMMAND ${PROTOC}
    ARGS --grpc_out="${CMAKE_CURRENT_BINARY_DIR}"
         --cpp_out="${CMAKE_CURRENT_BINARY_DIR}"
         -I="${CMAKE_CURRENT_SOURCE_DIR}/proto"
         --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
         ${CLUSTER_PROTO_FILE}
    DEPENDS ${CLUSTER_PROTO_FILE}
    COMMENT "Generating cluster proto/grpc files"
)

# ── raft proto (new for P3) ───────────────────────────────────────────────────
set(RAFT_PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/raft.pb.cc")
set(RAFT_PROTO_HDRS "${CMAKE_CURRENT_BINARY_DIR}/raft.pb.h")
set(RAFT_GRPC_SRCS  "${CMAKE_CURRENT_BINARY_DIR}/raft.grpc.pb.cc")
set(RAFT_GRPC_HDRS  "${CMAKE_CURRENT_BINARY_DIR}/raft.grpc.pb.h")

add_custom_command(
    OUTPUT ${RAFT_PROTO_SRCS} ${RAFT_PROTO_HDRS} ${RAFT_GRPC_SRCS} ${RAFT_GRPC_HDRS}
    COMMAND ${PROTOC}
    ARGS --grpc_out="${CMAKE_CURRENT_BINARY_DIR}"
         --cpp_out="${CMAKE_CURRENT_BINARY_DIR}"
         -I="${CMAKE_CURRENT_SOURCE_DIR}/proto"
         --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
         ${RAFT_PROTO_FILE}
    DEPENDS ${RAFT_PROTO_FILE}
    COMMENT "Generating raft proto/grpc files"
)

# ── Proto libraries ───────────────────────────────────────────────────────────
add_library(kvstore_proto ${KVSTORE_PROTO_SRCS} ${KVSTORE_GRPC_SRCS})
add_library(cluster_proto ${CLUSTER_PROTO_SRCS} ${CLUSTER_GRPC_SRCS})
add_library(raft_proto    ${RAFT_PROTO_SRCS}    ${RAFT_GRPC_SRCS})

foreach(lib kvstore_proto cluster_proto raft_proto)
    target_link_libraries(${lib}
        ${GRPC_LIBRARIES}
        ${PROTOBUF_LIBRARIES}
        grpc++ grpc protobuf
    )
    target_include_directories(${lib} PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}
        ${GRPC_INCLUDE_DIRS}
        ${PROTOBUF_INCLUDE_DIRS}
    )
    target_link_directories(${lib} PUBLIC
        ${GRPC_LIBRARY_DIRS}
        ${PROTOBUF_LIBRARY_DIRS}
    )
endforeach()

# ── Server executable ─────────────────────────────────────────────────────────
add_executable(kvserver src/server.cpp)

target_include_directories(kvserver PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_BINARY_DIR}
    ${ROCKSDB_INCLUDE_DIRS}
)

target_link_libraries(kvserver
    kvstore_proto
    cluster_proto
    raft_proto
    grpc++ grpc protobuf
    ${ROCKSDB_LIBS}
    Threads::Threads
)

target_link_directories(kvserver PRIVATE ${ROCKSDB_LIBRARY_DIRS})

# ── Client executable ─────────────────────────────────────────────────────────
add_executable(kvclient src/client.cpp)

target_include_directories(kvclient PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
)

target_link_libraries(kvclient
    kvstore_proto
    cluster_proto
    grpc++ grpc protobuf
    Threads::Threads
)

# ── Manager executable ────────────────────────────────────────────────────────
add_executable(kvmanager src/manager.cpp)

target_include_directories(kvmanager PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src   # for raft.h
    ${CMAKE_CURRENT_BINARY_DIR}
)

target_link_libraries(kvmanager
    cluster_proto
    raft_proto          # Raft gRPC stubs used by replicated manager
    grpc++ grpc protobuf
    Threads::Threads
)

# ── Install ───────────────────────────────────────────────────────────────────
install(TARGETS kvserver kvclient kvmanager
    RUNTIME DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/bin
)
