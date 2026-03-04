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

message(STATUS "Using gRPC ${GRPC_VERSION}")
message(STATUS "Using Protobuf ${PROTOBUF_VERSION}")

# Proto file
set(PROTO_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/kvstore.proto
)

# Generate gRPC and protobuf files
set(PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/kvstore.pb.cc")
set(PROTO_HDRS "${CMAKE_CURRENT_BINARY_DIR}/kvstore.pb.h")
set(GRPC_SRCS "${CMAKE_CURRENT_BINARY_DIR}/kvstore.grpc.pb.cc")
set(GRPC_HDRS "${CMAKE_CURRENT_BINARY_DIR}/kvstore.grpc.pb.h")

# Find protoc and grpc_cpp_plugin executables
find_program(PROTOC protoc REQUIRED)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
    COMMAND ${PROTOC}
    ARGS --grpc_out="${CMAKE_CURRENT_BINARY_DIR}"
         --cpp_out="${CMAKE_CURRENT_BINARY_DIR}"
         -I="${CMAKE_CURRENT_SOURCE_DIR}/proto"
         --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
         ${PROTO_FILES}
    DEPENDS ${PROTO_FILES}
    COMMENT "Generating gRPC and Protocol Buffer files"
)

# Common library with generated protobuf code
add_library(kvstore_proto
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)

target_link_libraries(kvstore_proto
    ${GRPC_LIBRARIES}
    ${PROTOBUF_LIBRARIES}
    grpc++
    grpc
    protobuf
)

target_include_directories(kvstore_proto PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR}
    ${GRPC_INCLUDE_DIRS}
    ${PROTOBUF_INCLUDE_DIRS}
)

target_link_directories(kvstore_proto PUBLIC
    ${GRPC_LIBRARY_DIRS}
    ${PROTOBUF_LIBRARY_DIRS}
)

# Server executable
add_executable(kvserver
    src/server.cpp
)

target_link_libraries(kvserver
    kvstore_proto
    grpc++
    grpc
    protobuf
    Threads::Threads
)

# Client executable
add_executable(kvclient
    src/client.cpp
)

target_link_libraries(kvclient
    kvstore_proto
    grpc++
    grpc
    protobuf
    Threads::Threads
)

# Install targets
install(TARGETS kvserver kvclient
    RUNTIME DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/bin
)
