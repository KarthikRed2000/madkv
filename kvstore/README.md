# KVStore C++ Implementation

## Architecture

- **Language**: C++17
- **RPC Framework**: gRPC
- **Data Structure**: `std::map` (maintains sorted order for SCAN)
- **Build System**: CMake

## Project Structure

```
kvstore/
├── CMakeLists.txt          # Build configuration
├── proto/
│   └── kvstore.proto       # gRPC service definition
├── src/
│   ├── server.cpp          # KV store server
│   └── client.cpp          # KV store client
└── bin/                    # Built executables (after build)
    ├── kvserver
    └── kvclient
```

## Building

```bash
# Install dependencies (Ubuntu 22.04)
just p1::deps

# Build executables
just p1::build

# Clean build artifacts
just p1::clean
```

## Running

```bash
# Start server
just p1::server 0.0.0.0:3777

# Run client (stdin/out mode)
just p1::client 127.0.0.1:3777

# Kill all processes
just p1::kill
```

## RPC Protocol

The gRPC service defines 5 operations:

1. **Put(key, value)** → found/not_found
2. **Swap(key, value)** → old_value or null
3. **Get(key)** → value or null
4. **Scan(start_key, end_key)** → list of key-value pairs
5. **Delete(key)** → found/not_found

See `proto/kvstore.proto` for detailed message definitions.

## Design Notes

- **Single-threaded**: Server handles requests sequentially (ensures linearizability)
- **In-memory storage**: Uses `std::map<string, string>` for ordered key storage
- **No durability**: All data is lost on server restart (as per Project 1 spec)
