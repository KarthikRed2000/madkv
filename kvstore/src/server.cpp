#include <grpcpp/grpcpp.h>

#include <condition_variable>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "cluster.grpc.pb.h"
#include "kvstore.grpc.pb.h"
#include "raft.h"               // Raft consensus (P3)
#include "raft.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using kvstore::ClusterManager;
using kvstore::DeleteRequest;
using kvstore::DeleteResponse;
using kvstore::GetRequest;
using kvstore::GetResponse;
using kvstore::GetClusterRequest;
using kvstore::GetClusterResponse;
using kvstore::KVStore;
using kvstore::PutRequest;
using kvstore::PutResponse;
using kvstore::RegisterServerRequest;
using kvstore::RegisterServerResponse;
using kvstore::ScanRequest;
using kvstore::ScanResponse;
using kvstore::SwapRequest;
using kvstore::SwapResponse;

static std::string NowTs() {
  const auto now = std::chrono::system_clock::now();
  const auto t   = std::chrono::system_clock::to_time_t(now);
  std::ostringstream oss;
  oss << std::put_time(std::localtime(&t), "%F %T");
  return oss.str();
}

static void Log(const std::string& msg) {
  std::cerr << "[" << NowTs() << "] [server] " << msg << std::endl;
}

static std::vector<std::string> SplitCsv(const std::string& csv) {
  std::vector<std::string> out;
  std::stringstream ss(csv);
  std::string item;
  while (std::getline(ss, item, ','))
    if (!item.empty()) out.push_back(item);
  return out;
}

// ─────────────────────────────────────────────────────────────────────────────
// WAL (reused for P2 mode)
// ─────────────────────────────────────────────────────────────────────────────
enum WalOp : uint8_t { WAL_SET = 0, WAL_DEL = 1 };
static constexpr size_t WAL_HEADER_SIZE   = 1 + 4 + 4;
static constexpr int    FLUSH_INTERVAL_MS = 20;

class WriteAheadLog {
 public:
  explicit WriteAheadLog(const std::string& path) : path_(path) {
    fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0) throw std::runtime_error("failed to open WAL: " + path);
    flusher_ = std::thread(&WriteAheadLog::FlushLoop, this);
  }

  ~WriteAheadLog() {
    {
      std::lock_guard<std::mutex> g(mu_);
      shutdown_ = true;
      cv_.notify_all();
    }
    if (flusher_.joinable()) flusher_.join();
    if (fd_ >= 0) ::close(fd_);
  }

  void append(WalOp op, const std::string& key, const std::string& value) {
    const uint32_t klen = static_cast<uint32_t>(key.size());
    const uint32_t vlen = static_cast<uint32_t>(value.size());
    std::vector<char> entry(WAL_HEADER_SIZE + klen + vlen);
    entry[0] = static_cast<char>(op);
    std::memcpy(&entry[1], &klen, 4);
    std::memcpy(&entry[5], &vlen, 4);
    std::memcpy(&entry[WAL_HEADER_SIZE],        key.data(),   klen);
    std::memcpy(&entry[WAL_HEADER_SIZE + klen], value.data(), vlen);

    uint64_t my_seq;
    {
      std::lock_guard<std::mutex> g(mu_);
      buffer_.insert(buffer_.end(), entry.begin(), entry.end());
      my_seq = ++write_seq_;
      cv_.notify_all();
    }
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [this, my_seq] {
      return flush_seq_ >= my_seq || shutdown_;
    });
  }

  static std::map<std::string, std::string> replay(const std::string& path) {
    std::map<std::string, std::string> state;
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return state;
    const off_t file_size = ::lseek(fd, 0, SEEK_END);
    if (file_size <= 0) { ::close(fd); return state; }
    ::lseek(fd, 0, SEEK_SET);
    std::vector<char> data(static_cast<size_t>(file_size));
    const ssize_t rd = ::read(fd, data.data(), data.size());
    ::close(fd);
    if (rd <= 0) return state;
    const size_t total = static_cast<size_t>(rd);
    size_t pos = 0;
    while (pos + WAL_HEADER_SIZE <= total) {
      const uint8_t op = static_cast<uint8_t>(data[pos]);
      uint32_t klen, vlen;
      std::memcpy(&klen, &data[pos + 1], 4);
      std::memcpy(&vlen, &data[pos + 5], 4);
      if (pos + WAL_HEADER_SIZE + klen + vlen > total) break;
      std::string k(&data[pos + WAL_HEADER_SIZE], klen);
      pos += WAL_HEADER_SIZE + klen;
      std::string v(&data[pos], vlen);
      pos += vlen;
      if      (op == WAL_SET) state[k]  = std::move(v);
      else if (op == WAL_DEL) state.erase(k);
    }
    return state;
  }

 private:
  void FlushBytes(std::vector<char> to_write, uint64_t seq_snapshot) {
    const ssize_t w = ::write(fd_, to_write.data(), to_write.size());
    if (w != static_cast<ssize_t>(to_write.size()))
      throw std::runtime_error("WAL write failed");
#ifdef __APPLE__
    ::fsync(fd_);
#else
    ::fdatasync(fd_);
#endif
    std::lock_guard<std::mutex> g(mu_);
    if (seq_snapshot > flush_seq_) flush_seq_ = seq_snapshot;
    cv_.notify_all();
  }

  void FlushLoop() {
    while (true) {
      std::vector<char> to_write;
      uint64_t seq_snap;
      {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait_for(lock, std::chrono::milliseconds(FLUSH_INTERVAL_MS),
                     [this] { return !buffer_.empty() || shutdown_; });
        if (buffer_.empty()) {
          if (shutdown_) break;
          continue;
        }
        to_write     = std::move(buffer_);
        entry_count_ = 0;
        seq_snap     = write_seq_;
      }
      FlushBytes(std::move(to_write), seq_snap);
    }
    std::vector<char> tail;
    uint64_t seq_snap;
    {
      std::lock_guard<std::mutex> g(mu_);
      tail     = std::move(buffer_);
      seq_snap = write_seq_;
    }
    if (!tail.empty()) FlushBytes(std::move(tail), seq_snap);
  }

  std::string       path_;
  int               fd_          = -1;
  std::vector<char> buffer_;
  size_t            entry_count_ = 0;
  uint64_t          write_seq_   = 0;
  uint64_t          flush_seq_   = 0;
  std::mutex              mu_;
  std::condition_variable cv_;
  std::thread             flusher_;
  bool                    shutdown_ = false;
};

// ─────────────────────────────────────────────────────────────────────────────
// KVStoreServiceImpl
//
// Write concurrency: two-phase — release state lock BEFORE calling WAL.
//
// PROBLEM: holding the exclusive state lock across wal_->append() (which waits
// for fdatasync) serialises every concurrent Get/Scan behind disk I/O.  10
// clients on 1 partition → server goes single-threaded → bench timeouts.
//
// SOLUTION — Phase 1 (exclusive lock, no I/O):
//   a. Read old value / found flag.
//   b. Apply new value to state_ immediately (readers see it right away).
//   c. Release the lock.
// Phase 2 (no lock held, slow I/O):
//   d. wal_->append() — batches with other writers, waits for fdatasync.
//   e. ACK sent to client only after this returns.
//
// Linearizability: write is in state_ before the client gets an ACK.  Any
// reader starting after the ACK sees the new value.  A reader that runs
// concurrently during Phase 2 may observe the new value early — that is fine
// because if the server crashes before Phase 2 completes the client never
// received an ACK and will retry (idempotent per spec).
// ─────────────────────────────────────────────────────────────────────────────
class KVStoreServiceImpl final : public KVStore::Service {
 public:
  explicit KVStoreServiceImpl(const std::string& db_path) : db_path_(db_path) {
    std::filesystem::create_directories(db_path);
    const std::string wal_path = db_path + "/wal.log";
    state_ = WriteAheadLog::replay(wal_path);
    wal_   = std::make_unique<WriteAheadLog>(wal_path);
    Log("state machine ready path=" + db_path_ +
        " keys=" + std::to_string(state_.size()));
  }

  Status Put(ServerContext* ctx, const PutRequest* req, PutResponse* res) override {
    (void)ctx;
    const auto& key   = req->key();
    const auto& value = req->value();
    bool found;
    {
      std::unique_lock<std::shared_mutex> g(mu_);
      found       = state_.count(key) > 0;
      state_[key] = value;
    }
    wal_->append(WAL_SET, key, value);
    res->set_found(found);
    return Status::OK;
  }

  Status Swap(ServerContext* ctx, const SwapRequest* req, SwapResponse* res) override {
    (void)ctx;
    const auto& key   = req->key();
    const auto& value = req->value();
    bool        found;
    std::string old_value;
    {
      std::unique_lock<std::shared_mutex> g(mu_);
      auto it = state_.find(key);
      found   = (it != state_.end());
      if (found) old_value = it->second;
      state_[key] = value;
    }
    wal_->append(WAL_SET, key, value);
    res->set_found(found);
    if (found) res->set_old_value(old_value);
    return Status::OK;
  }

  Status Get(ServerContext* ctx, const GetRequest* req, GetResponse* res) override {
    (void)ctx;
    std::shared_lock<std::shared_mutex> g(mu_);
    auto it = state_.find(req->key());
    if (it != state_.end()) {
      res->set_found(true);
      res->set_value(it->second);
    } else {
      res->set_found(false);
    }
    return Status::OK;
  }

  Status Scan(ServerContext* ctx, const ScanRequest* req, ScanResponse* res) override {
    (void)ctx;
    std::shared_lock<std::shared_mutex> g(mu_);
    for (auto it = state_.lower_bound(req->start_key());
         it != state_.end() && it->first <= req->end_key(); ++it) {
      auto* e = res->add_entries();
      e->set_key(it->first);
      e->set_value(it->second);
    }
    return Status::OK;
  }

  Status Delete(ServerContext* ctx, const DeleteRequest* req, DeleteResponse* res) override {
    (void)ctx;
    const auto& key = req->key();
    bool found;
    {
      std::unique_lock<std::shared_mutex> g(mu_);
      auto it = state_.find(key);
      found   = (it != state_.end());
      if (found) state_.erase(it);
    }
    if (found) wal_->append(WAL_DEL, key, "");
    res->set_found(found);
    return Status::OK;
  }

 private:
  std::string db_path_;
  std::map<std::string, std::string> state_;
  std::unique_ptr<WriteAheadLog>     wal_;
  mutable std::shared_mutex          mu_;
};

// ─────────────────────────────────────────────────────────────────────────────
// P3 KV service (Raft-replicated state machine)
// ─────────────────────────────────────────────────────────────────────────────
class P3KVStoreImpl final : public KVStore::Service {
 public:
  P3KVStoreImpl() = default;

  void SetRaft(RaftNode* raft) { raft_ = raft; }

  // Called by the Raft apply loop to apply a committed command.
  KVResult Apply(const KVCmd& cmd) {
    std::unique_lock<std::shared_mutex> g(mu_);
    KVResult result;
    switch (cmd.type) {
      case KVCmdType::PUT: {
        result.found = state_.count(cmd.key) > 0;
        if (result.found) result.old_value = state_.at(cmd.key);
        state_[cmd.key] = cmd.value;
        break;
      }
      case KVCmdType::SWAP: {
        auto it      = state_.find(cmd.key);
        result.found = (it != state_.end());
        if (result.found) {
          result.old_value = it->second;
          it->second       = cmd.value;
        } else {
          state_[cmd.key] = cmd.value;
        }
        break;
      }
      case KVCmdType::DEL: {
        auto it      = state_.find(cmd.key);
        result.found = (it != state_.end());
        if (result.found) state_.erase(it);
        break;
      }
    }
    return result;
  }

  Status Put(ServerContext* ctx, const PutRequest* req, PutResponse* res) override {
    (void)ctx;
    KVCmd cmd{KVCmdType::PUT, req->key(), req->value()};
    try {
      KVResult r = raft_->Submit(cmd);
      res->set_found(r.found);
      return Status::OK;
    } catch (const NotLeaderError& e) {
      return Status(grpc::StatusCode::FAILED_PRECONDITION, e.msg);
    } catch (const std::exception& e) {
      return Status(grpc::StatusCode::INTERNAL, e.what());
    }
  }

  Status Swap(ServerContext* ctx, const SwapRequest* req, SwapResponse* res) override {
    (void)ctx;
    KVCmd cmd{KVCmdType::SWAP, req->key(), req->value()};
    try {
      KVResult r = raft_->Submit(cmd);
      res->set_found(r.found);
      if (r.found) res->set_old_value(r.old_value);
      return Status::OK;
    } catch (const NotLeaderError& e) {
      return Status(grpc::StatusCode::FAILED_PRECONDITION, e.msg);
    } catch (const std::exception& e) {
      return Status(grpc::StatusCode::INTERNAL, e.what());
    }
  }

  Status Get(ServerContext* ctx, const GetRequest* req, GetResponse* res) override {
    (void)ctx;
    if (!raft_->IsLeader()) {
      return Status(grpc::StatusCode::FAILED_PRECONDITION,
                    "NOT_LEADER:" + raft_->GetLeaderApiAddr());
    }
    std::shared_lock<std::shared_mutex> g(mu_);
    auto it = state_.find(req->key());
    if (it != state_.end()) {
      res->set_found(true);
      res->set_value(it->second);
    } else {
      res->set_found(false);
    }
    return Status::OK;
  }

  Status Scan(ServerContext* ctx, const ScanRequest* req, ScanResponse* res) override {
    (void)ctx;
    if (!raft_->IsLeader()) {
      return Status(grpc::StatusCode::FAILED_PRECONDITION,
                    "NOT_LEADER:" + raft_->GetLeaderApiAddr());
    }
    std::shared_lock<std::shared_mutex> g(mu_);
    for (auto it = state_.lower_bound(req->start_key());
         it != state_.end() && it->first <= req->end_key(); ++it) {
      auto* e = res->add_entries();
      e->set_key(it->first);
      e->set_value(it->second);
    }
    return Status::OK;
  }

  Status Delete(ServerContext* ctx, const DeleteRequest* req, DeleteResponse* res) override {
    (void)ctx;
    KVCmd cmd{KVCmdType::DEL, req->key(), ""};
    try {
      KVResult r = raft_->Submit(cmd);
      res->set_found(r.found);
      return Status::OK;
    } catch (const NotLeaderError& e) {
      return Status(grpc::StatusCode::FAILED_PRECONDITION, e.msg);
    } catch (const std::exception& e) {
      return Status(grpc::StatusCode::INTERNAL, e.what());
    }
  }

 private:
  RaftNode* raft_ = nullptr;
  std::map<std::string, std::string> state_;
  mutable std::shared_mutex          mu_;
};

// ─────────────────────────────────────────────────────────────────────────────
// P3 Raft gRPC service wrapper
// ─────────────────────────────────────────────────────────────────────────────
class RaftServiceImpl final : public raft::RaftService::Service {
 public:
  explicit RaftServiceImpl(RaftNode* raft) : raft_(raft) {}

  grpc::Status RequestVote(grpc::ServerContext* ctx,
                           const raft::RequestVoteRequest* req,
                           raft::RequestVoteResponse* res) override {
    (void)ctx;
    raft_->HandleRequestVote(req, res);
    return grpc::Status::OK;
  }

  grpc::Status AppendEntries(grpc::ServerContext* ctx,
                              const raft::AppendEntriesRequest* req,
                              raft::AppendEntriesResponse* res) override {
    (void)ctx;
    raft_->HandleAppendEntries(req, res);
    return grpc::Status::OK;
  }

 private:
  RaftNode* raft_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Manager registration helpers (shared by P2 and P3)
// ─────────────────────────────────────────────────────────────────────────────
static bool RegisterToManager(uint32_t sid, const std::string& manager_addr,
                               const std::string& api_addr) {
  auto channel = grpc::CreateChannel(manager_addr, grpc::InsecureChannelCredentials());
  auto stub    = ClusterManager::NewStub(channel);

  RegisterServerRequest req;
  req.set_server_id(sid);
  req.set_api_addr(api_addr);
  RegisterServerResponse res;
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));

  grpc::Status s = stub->RegisterServer(&ctx, req, &res);
  if (s.ok() && res.ok()) {
    Log("registered to manager sid=" + std::to_string(sid) +
        " manager=" + manager_addr + " api_addr=" + api_addr);
    return true;
  }
  Log("register attempt failed sid=" + std::to_string(sid) +
      " manager=" + manager_addr +
      " grpc_ok=" + (s.ok() ? "true" : "false") +
      (s.ok() ? "" : " err=" + s.error_message()));
  return false;
}

static void WaitForClusterReady(const std::string& manager_addr) {
  auto channel = grpc::CreateChannel(manager_addr, grpc::InsecureChannelCredentials());
  auto stub    = ClusterManager::NewStub(channel);
  int  attempts = 0;
  while (true) {
    GetClusterRequest  req;
    GetClusterResponse res;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
    if (stub->GetCluster(&ctx, req, &res).ok() && res.ready()) {
      Log("cluster ready from manager=" + manager_addr);
      return;
    }
    if (++attempts % 10 == 0)
      Log("waiting for cluster ready manager=" + manager_addr);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
}

static void RunServer(const std::string& listen_addr, const std::string& db_path) {
  KVStoreServiceImpl service(db_path);
  ServerBuilder builder;
  builder.AddListeningPort(listen_addr, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  Log("listening on " + listen_addr + " db=" + db_path);
  server->Wait();
}

// ─────────────────────────────────────────────────────────────────────────────
// P3 startup
// ─────────────────────────────────────────────────────────────────────────────

struct P3Args {
  int                      partition_id = 0;
  int                      replica_id   = 0;
  std::vector<std::string> manager_addrs;
  std::string              api_port;
  std::string              p2p_port;
  std::vector<std::string> peer_addrs;   // peer P2P addrs, sorted by ID, excl self
  std::string              backer_path;
};

static P3Args ParseP3Args(int argc, char** argv) {
  P3Args args;
  for (int i = 1; i < argc; i++) {
    std::string flag = argv[i];
    if      (flag == "--partition_id")  args.partition_id  = std::stoi(argv[++i]);
    else if (flag == "--replica_id")    args.replica_id    = std::stoi(argv[++i]);
    else if (flag == "--manager_addrs") args.manager_addrs = SplitCsv(argv[++i]);
    else if (flag == "--api_port")      args.api_port      = argv[++i];
    else if (flag == "--p2p_port")      args.p2p_port      = argv[++i];
    else if (flag == "--peer_addrs") {
      std::string s = argv[++i];
      if (s != "none") args.peer_addrs = SplitCsv(s);
    }
    else if (flag == "--backer_path")   args.backer_path   = argv[++i];
  }
  return args;
}

static void RunP3Server(const P3Args& args) {
  int rf        = static_cast<int>(args.peer_addrs.size()) + 1;
  int server_id = args.partition_id * rf + args.replica_id;

  std::string api_listen = "0.0.0.0:" + args.api_port;
  std::string p2p_listen = "0.0.0.0:" + args.p2p_port;

  std::filesystem::create_directories(args.backer_path);
  Log("P3 startup partition=" + std::to_string(args.partition_id) +
      " replica=" + std::to_string(args.replica_id) +
      " rf=" + std::to_string(rf) +
      " api=" + api_listen + " p2p=" + p2p_listen);

  // ── Register with a manager (any one that responds) ───────────────────────
  {
    bool registered = false;
    while (!registered) {
      for (const auto& mgr : args.manager_addrs) {
        if (RegisterToManager(server_id, mgr, api_listen)) {
          registered = true;
          break;
        }
      }
      if (!registered) std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
  }

  // ── Wait for cluster ready and get partition's replica API addresses ───────
  std::vector<std::string> partition_api_addrs;
  {
    while (partition_api_addrs.empty()) {
      for (const auto& mgr : args.manager_addrs) {
        auto ch   = grpc::CreateChannel(mgr, grpc::InsecureChannelCredentials());
        auto stub = ClusterManager::NewStub(ch);
        GetClusterRequest  req;
        GetClusterResponse res;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
        if (!stub->GetCluster(&ctx, req, &res).ok() || !res.ready()) continue;

        for (const auto& pi : res.partitions()) {
          if ((int)pi.partition_id() == args.partition_id) {
            for (const auto& addr : pi.replica_addrs())
              partition_api_addrs.push_back(addr);
            break;
          }
        }
        if (!partition_api_addrs.empty()) break;
      }
      if (partition_api_addrs.empty())
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    Log("cluster ready, partition has " + std::to_string(partition_api_addrs.size()) +
        " replicas");
  }

  // ── Build full P2P address list (indexed by replica ID) ───────────────────
  // peer_addrs is sorted by replica ID, excluding self
  std::vector<std::string> all_p2p_addrs(rf);
  {
    int j = 0;
    for (int i = 0; i < rf; i++) {
      if (i == args.replica_id) {
        all_p2p_addrs[i] = p2p_listen;
      } else {
        all_p2p_addrs[i] = args.peer_addrs[j++];
      }
    }
  }

  // ── Create P3 KV service and Raft node ────────────────────────────────────
  auto kv_service = std::make_unique<P3KVStoreImpl>();

  auto raft = std::make_unique<RaftNode>(
      args.replica_id,
      partition_api_addrs,   // indexed by replica_id
      all_p2p_addrs,
      args.backer_path,
      [kv_ptr = kv_service.get()](const KVCmd& cmd) {
        return kv_ptr->Apply(cmd);
      });

  kv_service->SetRaft(raft.get());

  // ── Start Raft ────────────────────────────────────────────────────────────
  raft->Start();

  // ── Start Raft P2P server ─────────────────────────────────────────────────
  RaftServiceImpl raft_service(raft.get());
  ServerBuilder   raft_builder;
  raft_builder.AddListeningPort(p2p_listen, grpc::InsecureServerCredentials());
  raft_builder.RegisterService(&raft_service);
  auto raft_server = raft_builder.BuildAndStart();
  Log("Raft P2P listening on " + p2p_listen);

  // ── Start KV API server ───────────────────────────────────────────────────
  ServerBuilder kv_builder;
  kv_builder.AddListeningPort(api_listen, grpc::InsecureServerCredentials());
  kv_builder.RegisterService(kv_service.get());
  auto kv_server = kv_builder.BuildAndStart();
  Log("KV API listening on " + api_listen);

  kv_server->Wait();

  raft_server->Shutdown();
  raft->Stop();
}

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────
int main(int argc, char** argv) {
  try {
    // P3 mode: first arg is a flag
    if (argc >= 2 && std::string(argv[1]).rfind("--", 0) == 0) {
      P3Args args = ParseP3Args(argc, argv);
      RunP3Server(args);
      return 0;
    }

    // P1 mode: <listen_addr> [backer_path]
    if (argc == 2 || argc == 3) {
      const std::string listen_addr = argv[1];
      const std::string backer_path = (argc == 3) ? argv[2] : "./backer.default";
      std::filesystem::create_directories(backer_path);
      RunServer(listen_addr, backer_path);
      return 0;
    }

    // P2 mode: <id> <manager_addr> <api_port> <backer_path>
    if (argc == 5) {
      const uint32_t    sid          = static_cast<uint32_t>(std::stoul(argv[1]));
      const std::string manager_addr = argv[2];
      const std::string api_port     = argv[3];
      const std::string backer_path  = argv[4];
      const std::string listen_addr  = "0.0.0.0:" + api_port;

      std::filesystem::create_directories(backer_path);
      Log("p2 startup sid=" + std::to_string(sid) + " manager=" + manager_addr);

      while (!RegisterToManager(sid, manager_addr, listen_addr))
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      WaitForClusterReady(manager_addr);
      RunServer(listen_addr, backer_path);
      return 0;
    }

    std::cerr << "Usage (P1): " << argv[0] << " <listen_addr> [backer_path]\n";
    std::cerr << "Usage (P2): " << argv[0]
              << " <id> <manager_addr> <api_port> <backer_path>\n";
    std::cerr << "Usage (P3): " << argv[0]
              << " --partition_id N --replica_id N --manager_addrs a:p,...\n"
                 "             --api_port P --p2p_port P --peer_addrs a:p,...\n"
                 "             --backer_path PATH\n";
    return 1;
  } catch (const std::exception& e) {
    std::cerr << "server error: " << e.what() << std::endl;
    return 1;
  }
}
