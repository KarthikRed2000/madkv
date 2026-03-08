#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "cluster.grpc.pb.h"
#include "kvstore.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using kvstore::ClusterManager;
using kvstore::DeleteRequest;
using kvstore::DeleteResponse;
using kvstore::GetClusterRequest;
using kvstore::GetClusterResponse;
using kvstore::GetRequest;
using kvstore::GetResponse;
using kvstore::KVStore;
using kvstore::PutRequest;
using kvstore::PutResponse;
using kvstore::ScanRequest;
using kvstore::ScanResponse;
using kvstore::SwapRequest;
using kvstore::SwapResponse;

static uint64_t StableHash(const std::string& s) {
  // FNV-1a 64-bit
  uint64_t h = 1469598103934665603ULL;
  for (unsigned char c : s) {
    h ^= static_cast<uint64_t>(c);
    h *= 1099511628211ULL;
  }
  return h;
}

class DirectClient {
 public:
  explicit DirectClient(const std::string& server_addr)
      : stub_(KVStore::NewStub(
            grpc::CreateChannel(server_addr, grpc::InsecureChannelCredentials()))) {}

  void Put(const std::string& key, const std::string& value) {
    PutRequest req;
    req.set_key(key);
    req.set_value(value);
    PutResponse res;
    DoWithRetry([&](ClientContext& ctx) { return stub_->Put(&ctx, req, &res); });
    std::cout << "PUT " << key << " " << (res.found() ? "found" : "not_found")
              << std::endl;
  }

  void Swap(const std::string& key, const std::string& value) {
    SwapRequest req;
    req.set_key(key);
    req.set_value(value);
    SwapResponse res;
    DoWithRetry([&](ClientContext& ctx) { return stub_->Swap(&ctx, req, &res); });
    std::cout << "SWAP " << key << " " << (res.found() ? res.old_value() : "null")
              << std::endl;
  }

  void Get(const std::string& key) {
    GetRequest req;
    req.set_key(key);
    GetResponse res;
    DoWithRetry([&](ClientContext& ctx) { return stub_->Get(&ctx, req, &res); });
    std::cout << "GET " << key << " " << (res.found() ? res.value() : "null")
              << std::endl;
  }

  void Delete(const std::string& key) {
    DeleteRequest req;
    req.set_key(key);
    DeleteResponse res;
    DoWithRetry([&](ClientContext& ctx) { return stub_->Delete(&ctx, req, &res); });
    std::cout << "DELETE " << key << " " << (res.found() ? "found" : "not_found")
              << std::endl;
  }

  void Scan(const std::string& start_key, const std::string& end_key) {
    ScanRequest req;
    req.set_start_key(start_key);
    req.set_end_key(end_key);
    ScanResponse res;
    DoWithRetry([&](ClientContext& ctx) { return stub_->Scan(&ctx, req, &res); });
    PrintScan(start_key, end_key, res);
  }

 private:
  template <typename RpcFn>
  void DoWithRetry(RpcFn rpc) {
    while (true) {
      ClientContext ctx;
      ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(300));
      Status s = rpc(ctx);
      if (s.ok()) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }
  }

  static void PrintScan(const std::string& start_key, const std::string& end_key,
                        const ScanResponse& res) {
    std::cout << "SCAN " << start_key << " " << end_key << " BEGIN" << std::endl;
    for (const auto& e : res.entries()) {
      std::cout << "  " << e.key() << " " << e.value() << std::endl;
    }
    std::cout << "SCAN END" << std::endl;
  }

  std::unique_ptr<KVStore::Stub> stub_;
};

class PartitionedClient {
 public:
  explicit PartitionedClient(const std::string& manager_addr) : manager_addr_(manager_addr) {
    RefreshClusterWithRetry();
  }

  void Put(const std::string& key, const std::string& value) {
    PutRequest req;
    req.set_key(key);
    req.set_value(value);
    PutResponse res;
    const size_t p = KeyToPartition(key);
    DoOnPartitionWithRetry(p, [&](KVStore::Stub* s, ClientContext& ctx) {
      return s->Put(&ctx, req, &res);
    });
    std::cout << "PUT " << key << " " << (res.found() ? "found" : "not_found")
              << std::endl;
  }

  void Swap(const std::string& key, const std::string& value) {
    SwapRequest req;
    req.set_key(key);
    req.set_value(value);
    SwapResponse res;
    const size_t p = KeyToPartition(key);
    DoOnPartitionWithRetry(p, [&](KVStore::Stub* s, ClientContext& ctx) {
      return s->Swap(&ctx, req, &res);
    });
    std::cout << "SWAP " << key << " " << (res.found() ? res.old_value() : "null")
              << std::endl;
  }

  void Get(const std::string& key) {
    GetRequest req;
    req.set_key(key);
    GetResponse res;
    const size_t p = KeyToPartition(key);
    DoOnPartitionWithRetry(p, [&](KVStore::Stub* s, ClientContext& ctx) {
      return s->Get(&ctx, req, &res);
    });
    std::cout << "GET " << key << " " << (res.found() ? res.value() : "null")
              << std::endl;
  }

  void Delete(const std::string& key) {
    DeleteRequest req;
    req.set_key(key);
    DeleteResponse res;
    const size_t p = KeyToPartition(key);
    DoOnPartitionWithRetry(p, [&](KVStore::Stub* s, ClientContext& ctx) {
      return s->Delete(&ctx, req, &res);
    });
    std::cout << "DELETE " << key << " " << (res.found() ? "found" : "not_found")
              << std::endl;
  }

  void Scan(const std::string& start_key, const std::string& end_key) {
    std::vector<std::pair<std::string, std::string>> all_entries;
    for (size_t p = 0; p < stubs_.size(); ++p) {
      ScanRequest req;
      req.set_start_key(start_key);
      req.set_end_key(end_key);
      ScanResponse res;
      DoOnPartitionWithRetry(p, [&](KVStore::Stub* s, ClientContext& ctx) {
        return s->Scan(&ctx, req, &res);
      });
      for (const auto& e : res.entries()) {
        all_entries.emplace_back(e.key(), e.value());
      }
    }

    std::sort(all_entries.begin(), all_entries.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    std::cout << "SCAN " << start_key << " " << end_key << " BEGIN" << std::endl;
    for (const auto& [k, v] : all_entries) {
      std::cout << "  " << k << " " << v << std::endl;
    }
    std::cout << "SCAN END" << std::endl;
  }

 private:
  size_t KeyToPartition(const std::string& key) const {
    return static_cast<size_t>(StableHash(key) % stubs_.size());
  }

  void RefreshClusterWithRetry() {
    auto channel =
        grpc::CreateChannel(manager_addr_, grpc::InsecureChannelCredentials());
    auto manager_stub = ClusterManager::NewStub(channel);

    while (true) {
      GetClusterRequest req;
      GetClusterResponse res;
      ClientContext ctx;
      ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
      Status s = manager_stub->GetCluster(&ctx, req, &res);
      if (s.ok() && res.ready() && res.partitions_size() > 0) {
        stubs_.clear();
        for (const auto& p : res.partitions()) {
          auto ch = grpc::CreateChannel(p.api_addr(), grpc::InsecureChannelCredentials());
          stubs_.push_back(KVStore::NewStub(ch));
        }
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }
  }

  template <typename RpcFn>
  void DoOnPartitionWithRetry(size_t partition, RpcFn rpc) {
    while (true) {
      ClientContext ctx;
      ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(300));
      Status s = rpc(stubs_.at(partition).get(), ctx);
      if (s.ok()) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
      if (s.error_code() == grpc::StatusCode::UNAVAILABLE) {
        RefreshClusterWithRetry();
        // RefreshClusterWithRetry() only returns once stubs_ is non-empty
        // (it loops until res.partitions_size() > 0), so stubs_.size() >= 1
        // here and the subtraction is safe.  Clamp rather than re-hash so
        // that callers that already selected a partition keep targeting the
        // same logical shard if the cluster size hasn't changed.
        if (!stubs_.empty()) {
          partition = std::min(partition, stubs_.size() - 1);
        }
      }
    }
  }

  std::string manager_addr_;
  std::vector<std::unique_ptr<KVStore::Stub>> stubs_;
};

template <typename ClientType>
static void ProcessCommands(ClientType& client) {
  std::string line;
  while (std::getline(std::cin, line)) {
    if (line.empty() || line.find_first_not_of(" \t\n\r") == std::string::npos) {
      continue;
    }
    std::istringstream iss(line);
    std::string cmd;
    iss >> cmd;

    if (cmd == "PUT") {
      std::string key, value;
      iss >> key >> value;
      client.Put(key, value);
    } else if (cmd == "SWAP") {
      std::string key, value;
      iss >> key >> value;
      client.Swap(key, value);
    } else if (cmd == "GET") {
      std::string key;
      iss >> key;
      client.Get(key);
    } else if (cmd == "SCAN") {
      std::string start_key, end_key;
      iss >> start_key >> end_key;
      client.Scan(start_key, end_key);
    } else if (cmd == "DELETE") {
      std::string key;
      iss >> key;
      client.Delete(key);
    } else if (cmd == "STOP") {
      std::cout << "STOP" << std::endl;
      break;
    } else {
      std::cerr << "Unknown command: " << cmd << std::endl;
    }
  }
}

int main(int argc, char** argv) {
  try {
    // P2 mode
    if (argc == 3 && std::string(argv[1]) == "--manager") {
      PartitionedClient client(argv[2]);
      ProcessCommands(client);
      return 0;
    }

    // P1 compatibility mode
    if (argc == 2) {
      DirectClient client(argv[1]);
      ProcessCommands(client);
      return 0;
    }

    std::cerr << "Usage (P1): " << argv[0] << " <server_addr>" << std::endl;
    std::cerr << "Usage (P2): " << argv[0] << " --manager <manager_addr>"
              << std::endl;
    return 1;
  } catch (const std::exception& e) {
    std::cerr << "client error: " << e.what() << std::endl;
    return 1;
  }
}