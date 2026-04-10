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
using grpc::StatusCode;

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

// ─────────────────────────────────────────────────────────────────────────────
// Utilities
// ─────────────────────────────────────────────────────────────────────────────

static uint64_t StableHash(const std::string& s) {
  uint64_t h = 1469598103934665603ULL;
  for (unsigned char c : s) {
    h ^= static_cast<uint64_t>(c);
    h *= 1099511628211ULL;
  }
  return h;
}

static std::vector<std::string> SplitCsv(const std::string& csv) {
  std::vector<std::string> out;
  std::stringstream ss(csv);
  std::string item;
  while (std::getline(ss, item, ','))
    if (!item.empty()) out.push_back(item);
  return out;
}

// Extract leader address from FAILED_PRECONDITION error message.
// Format: "NOT_LEADER:<addr>" or "NOT_LEADER:" (empty = unknown)
static std::string ParseLeaderHint(const std::string& msg) {
  const std::string prefix = "NOT_LEADER:";
  if (msg.size() > prefix.size() && msg.substr(0, prefix.size()) == prefix)
    return msg.substr(prefix.size());
  return "";
}

// ─────────────────────────────────────────────────────────────────────────────
// P1 direct client
// ─────────────────────────────────────────────────────────────────────────────
class DirectClient {
 public:
  explicit DirectClient(const std::string& server_addr)
      : stub_(KVStore::NewStub(
            grpc::CreateChannel(server_addr, grpc::InsecureChannelCredentials()))) {}

  void Put(const std::string& key, const std::string& value) {
    PutRequest req; req.set_key(key); req.set_value(value);
    PutResponse res;
    DoWithRetry([&](ClientContext& ctx) { return stub_->Put(&ctx, req, &res); });
    std::cout << "PUT " << key << " " << (res.found() ? "found" : "not_found") << std::endl;
  }

  void Swap(const std::string& key, const std::string& value) {
    SwapRequest req; req.set_key(key); req.set_value(value);
    SwapResponse res;
    DoWithRetry([&](ClientContext& ctx) { return stub_->Swap(&ctx, req, &res); });
    std::cout << "SWAP " << key << " " << (res.found() ? res.old_value() : "null") << std::endl;
  }

  void Get(const std::string& key) {
    GetRequest req; req.set_key(key);
    GetResponse res;
    DoWithRetry([&](ClientContext& ctx) { return stub_->Get(&ctx, req, &res); });
    std::cout << "GET " << key << " " << (res.found() ? res.value() : "null") << std::endl;
  }

  void Delete(const std::string& key) {
    DeleteRequest req; req.set_key(key);
    DeleteResponse res;
    DoWithRetry([&](ClientContext& ctx) { return stub_->Delete(&ctx, req, &res); });
    std::cout << "DELETE " << key << " " << (res.found() ? "found" : "not_found") << std::endl;
  }

  void Scan(const std::string& start_key, const std::string& end_key) {
    ScanRequest req; req.set_start_key(start_key); req.set_end_key(end_key);
    ScanResponse res;
    DoWithRetry([&](ClientContext& ctx) { return stub_->Scan(&ctx, req, &res); });
    PrintScan(start_key, end_key, res);
  }

 private:
  template <typename F>
  void DoWithRetry(F rpc) {
    while (true) {
      ClientContext ctx;
      ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(300));
      if (rpc(ctx).ok()) return;
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }
  }

  static void PrintScan(const std::string& s, const std::string& e, const ScanResponse& res) {
    std::cout << "SCAN " << s << " " << e << " BEGIN" << std::endl;
    for (const auto& entry : res.entries())
      std::cout << "  " << entry.key() << " " << entry.value() << std::endl;
    std::cout << "SCAN END" << std::endl;
  }

  std::unique_ptr<KVStore::Stub> stub_;
};

// ─────────────────────────────────────────────────────────────────────────────
// P2 partitioned client (single manager)
// ─────────────────────────────────────────────────────────────────────────────
class PartitionedClient {
 public:
  explicit PartitionedClient(const std::string& manager_addr) : manager_addr_(manager_addr) {
    RefreshClusterWithRetry();
  }

  void Put(const std::string& key, const std::string& value) {
    PutRequest req; req.set_key(key); req.set_value(value);
    PutResponse res;
    const size_t p = KeyToPartition(key);
    DoOnPartitionWithRetry(p, [&](KVStore::Stub* s, ClientContext& ctx) {
      return s->Put(&ctx, req, &res);
    });
    std::cout << "PUT " << key << " " << (res.found() ? "found" : "not_found") << std::endl;
  }

  void Swap(const std::string& key, const std::string& value) {
    SwapRequest req; req.set_key(key); req.set_value(value);
    SwapResponse res;
    const size_t p = KeyToPartition(key);
    DoOnPartitionWithRetry(p, [&](KVStore::Stub* s, ClientContext& ctx) {
      return s->Swap(&ctx, req, &res);
    });
    std::cout << "SWAP " << key << " " << (res.found() ? res.old_value() : "null") << std::endl;
  }

  void Get(const std::string& key) {
    GetRequest req; req.set_key(key);
    GetResponse res;
    const size_t p = KeyToPartition(key);
    DoOnPartitionWithRetry(p, [&](KVStore::Stub* s, ClientContext& ctx) {
      return s->Get(&ctx, req, &res);
    });
    std::cout << "GET " << key << " " << (res.found() ? res.value() : "null") << std::endl;
  }

  void Delete(const std::string& key) {
    DeleteRequest req; req.set_key(key);
    DeleteResponse res;
    const size_t p = KeyToPartition(key);
    DoOnPartitionWithRetry(p, [&](KVStore::Stub* s, ClientContext& ctx) {
      return s->Delete(&ctx, req, &res);
    });
    std::cout << "DELETE " << key << " " << (res.found() ? "found" : "not_found") << std::endl;
  }

  void Scan(const std::string& start_key, const std::string& end_key) {
    std::vector<std::pair<std::string, std::string>> all_entries;
    for (size_t p = 0; p < stubs_.size(); ++p) {
      ScanRequest req; req.set_start_key(start_key); req.set_end_key(end_key);
      ScanResponse res;
      DoOnPartitionWithRetry(p, [&](KVStore::Stub* s, ClientContext& ctx) {
        return s->Scan(&ctx, req, &res);
      });
      for (const auto& e : res.entries())
        all_entries.emplace_back(e.key(), e.value());
    }
    std::sort(all_entries.begin(), all_entries.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    std::cout << "SCAN " << start_key << " " << end_key << " BEGIN" << std::endl;
    for (const auto& [k, v] : all_entries)
      std::cout << "  " << k << " " << v << std::endl;
    std::cout << "SCAN END" << std::endl;
  }

 private:
  size_t KeyToPartition(const std::string& key) const {
    return static_cast<size_t>(StableHash(key) % stubs_.size());
  }

  void RefreshClusterWithRetry() {
    auto channel      = grpc::CreateChannel(manager_addr_, grpc::InsecureChannelCredentials());
    auto manager_stub = ClusterManager::NewStub(channel);
    while (true) {
      GetClusterRequest req;
      GetClusterResponse res;
      ClientContext ctx;
      ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
      if (manager_stub->GetCluster(&ctx, req, &res).ok() && res.ready() &&
          res.partitions_size() > 0) {
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

  template <typename F>
  void DoOnPartitionWithRetry(size_t partition, F rpc) {
    while (true) {
      ClientContext ctx;
      ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(300));
      Status s = rpc(stubs_.at(partition).get(), ctx);
      if (s.ok()) return;
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
      if (s.error_code() == StatusCode::UNAVAILABLE) {
        RefreshClusterWithRetry();
        if (!stubs_.empty())
          partition = std::min(partition, stubs_.size() - 1);
      }
    }
  }

  std::string manager_addr_;
  std::vector<std::unique_ptr<KVStore::Stub>> stubs_;
};

// ─────────────────────────────────────────────────────────────────────────────
// P3 replicated partitioned client
//
// Per partition: maintains a list of all replica API addresses and tracks
// which one is the current leader. Handles FAILED_PRECONDITION redirects and
// connection failures transparently.
// ─────────────────────────────────────────────────────────────────────────────

struct PartitionGroup {
  std::vector<std::string>               addrs;   // all replica API addrs
  std::vector<std::unique_ptr<KVStore::Stub>> stubs;
  int leader_idx = 0;

  KVStore::Stub* Leader() { return stubs.at(leader_idx).get(); }

  void SetLeaderByAddr(const std::string& addr) {
    for (int i = 0; i < (int)addrs.size(); i++) {
      if (addrs[i] == addr) { leader_idx = i; return; }
    }
    // addr not found — maybe we're using "0.0.0.0:…" vs a real IP; advance anyway
    Advance();
  }

  void Advance() {
    leader_idx = (leader_idx + 1) % static_cast<int>(stubs.size());
  }
};

class ReplicatedPartitionedClient {
 public:
  explicit ReplicatedPartitionedClient(std::vector<std::string> manager_addrs)
      : manager_addrs_(std::move(manager_addrs)) {
    RefreshCluster();
  }

  void Put(const std::string& key, const std::string& value) {
    PutRequest req; req.set_key(key); req.set_value(value);
    PutResponse res;
    size_t p = KeyToPartition(key);
    DoWithLeaderRetry(p, [&](KVStore::Stub* s, ClientContext& ctx) {
      return s->Put(&ctx, req, &res);
    });
    std::cout << "PUT " << key << " " << (res.found() ? "found" : "not_found") << std::endl;
  }

  void Swap(const std::string& key, const std::string& value) {
    SwapRequest req; req.set_key(key); req.set_value(value);
    SwapResponse res;
    size_t p = KeyToPartition(key);
    DoWithLeaderRetry(p, [&](KVStore::Stub* s, ClientContext& ctx) {
      return s->Swap(&ctx, req, &res);
    });
    std::cout << "SWAP " << key << " " << (res.found() ? res.old_value() : "null") << std::endl;
  }

  void Get(const std::string& key) {
    GetRequest req; req.set_key(key);
    GetResponse res;
    size_t p = KeyToPartition(key);
    DoWithLeaderRetry(p, [&](KVStore::Stub* s, ClientContext& ctx) {
      return s->Get(&ctx, req, &res);
    });
    std::cout << "GET " << key << " " << (res.found() ? res.value() : "null") << std::endl;
  }

  void Delete(const std::string& key) {
    DeleteRequest req; req.set_key(key);
    DeleteResponse res;
    size_t p = KeyToPartition(key);
    DoWithLeaderRetry(p, [&](KVStore::Stub* s, ClientContext& ctx) {
      return s->Delete(&ctx, req, &res);
    });
    std::cout << "DELETE " << key << " " << (res.found() ? "found" : "not_found") << std::endl;
  }

  void Scan(const std::string& start_key, const std::string& end_key) {
    std::vector<std::pair<std::string, std::string>> all_entries;
    for (size_t p = 0; p < partitions_.size(); ++p) {
      ScanRequest req; req.set_start_key(start_key); req.set_end_key(end_key);
      ScanResponse res;
      DoWithLeaderRetry(p, [&](KVStore::Stub* s, ClientContext& ctx) {
        return s->Scan(&ctx, req, &res);
      });
      for (const auto& e : res.entries())
        all_entries.emplace_back(e.key(), e.value());
    }
    std::sort(all_entries.begin(), all_entries.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    std::cout << "SCAN " << start_key << " " << end_key << " BEGIN" << std::endl;
    for (const auto& [k, v] : all_entries)
      std::cout << "  " << k << " " << v << std::endl;
    std::cout << "SCAN END" << std::endl;
  }

 private:
  std::vector<std::string>   manager_addrs_;
  std::vector<PartitionGroup> partitions_;

  size_t KeyToPartition(const std::string& key) const {
    return static_cast<size_t>(StableHash(key) % partitions_.size());
  }

  void RefreshCluster() {
    while (true) {
      for (const auto& mgr : manager_addrs_) {
        auto ch   = grpc::CreateChannel(mgr, grpc::InsecureChannelCredentials());
        auto stub = ClusterManager::NewStub(ch);
        GetClusterRequest  req;
        GetClusterResponse res;
        ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
        if (!stub->GetCluster(&ctx, req, &res).ok()) continue;
        if (!res.ready() || res.partitions_size() == 0) continue;

        partitions_.clear();
        for (const auto& pi : res.partitions()) {
          PartitionGroup pg;
          // Use replica_addrs if available (P3), else fall back to api_addr (P2)
          if (pi.replica_addrs_size() > 0) {
            for (const auto& addr : pi.replica_addrs()) pg.addrs.push_back(addr);
          } else {
            pg.addrs.push_back(pi.api_addr());
          }
          for (const auto& addr : pg.addrs) {
            auto c = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
            pg.stubs.push_back(KVStore::NewStub(c));
          }
          pg.leader_idx = 0;
          partitions_.push_back(std::move(pg));
        }
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
  }

  // Retry an RPC on the leader of a partition.
  // Handles: FAILED_PRECONDITION (redirect), UNAVAILABLE (connection failure).
  template <typename F>
  void DoWithLeaderRetry(size_t partition, F rpc) {
    auto& pg            = partitions_[partition];
    int   max_attempts  = static_cast<int>(pg.stubs.size()) * 6 + 10;

    for (int attempt = 0; attempt < max_attempts; attempt++) {
      ClientContext ctx;
      ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
      Status s = rpc(pg.Leader(), ctx);

      if (s.ok()) return;

      if (s.error_code() == StatusCode::FAILED_PRECONDITION) {
        std::string hint = ParseLeaderHint(s.error_message());
        if (!hint.empty()) {
          pg.SetLeaderByAddr(hint);
        } else {
          // Leader unknown, try next
          pg.Advance();
          std::this_thread::sleep_for(std::chrono::milliseconds(300));
        }
      } else if (s.error_code() == StatusCode::UNAVAILABLE ||
                 s.error_code() == StatusCode::DEADLINE_EXCEEDED) {
        pg.Advance();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      } else {
        // Other error: brief wait, then retry
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
      }

      // After cycling through all replicas twice, refresh cluster info
      if (attempt > 0 && attempt % (static_cast<int>(pg.stubs.size()) * 2) == 0) {
        RefreshCluster();
      }
    }
    // Last resort: refresh cluster and try once more
    RefreshCluster();
    ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
    rpc(partitions_[partition].Leader(), ctx);
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// Command loop (shared by all client types)
// ─────────────────────────────────────────────────────────────────────────────
template <typename ClientType>
static void ProcessCommands(ClientType& client) {
  std::string line;
  while (std::getline(std::cin, line)) {
    if (line.empty() || line.find_first_not_of(" \t\n\r") == std::string::npos) continue;
    std::istringstream iss(line);
    std::string cmd;
    iss >> cmd;

    if      (cmd == "PUT")    { std::string k, v; iss >> k >> v; client.Put(k, v); }
    else if (cmd == "SWAP")   { std::string k, v; iss >> k >> v; client.Swap(k, v); }
    else if (cmd == "GET")    { std::string k;    iss >> k;      client.Get(k); }
    else if (cmd == "DELETE") { std::string k;    iss >> k;      client.Delete(k); }
    else if (cmd == "SCAN")   {
      std::string s, e;
      iss >> s >> e;
      client.Scan(s, e);
    }
    else if (cmd == "STOP") { std::cout << "STOP" << std::endl; break; }
    else std::cerr << "Unknown command: " << cmd << std::endl;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────
int main(int argc, char** argv) {
  try {
    // P3 mode: --manager_addrs addr1,addr2,...
    if (argc == 3 && std::string(argv[1]) == "--manager_addrs") {
      auto addrs = SplitCsv(argv[2]);
      ReplicatedPartitionedClient client(std::move(addrs));
      ProcessCommands(client);
      return 0;
    }

    // P2 mode: --manager <addr>
    if (argc == 3 && std::string(argv[1]) == "--manager") {
      PartitionedClient client(argv[2]);
      ProcessCommands(client);
      return 0;
    }

    // P1 mode: <server_addr>
    if (argc == 2) {
      DirectClient client(argv[1]);
      ProcessCommands(client);
      return 0;
    }

    std::cerr << "Usage (P1): " << argv[0] << " <server_addr>\n";
    std::cerr << "Usage (P2): " << argv[0] << " --manager <manager_addr>\n";
    std::cerr << "Usage (P3): " << argv[0] << " --manager_addrs addr1,addr2,...\n";
    return 1;
  } catch (const std::exception& e) {
    std::cerr << "client error: " << e.what() << std::endl;
    return 1;
  }
}
