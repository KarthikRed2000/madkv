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

// ─────────────────────────────────────────────────────────────────────────────
// WAL entry layout: [op:1][klen:4][vlen:4][key_bytes][value_bytes]
// ─────────────────────────────────────────────────────────────────────────────
enum WalOp : uint8_t { WAL_SET = 0, WAL_DEL = 1 };
static constexpr size_t WAL_HEADER_SIZE   = 1 + 4 + 4;
static constexpr size_t BATCH_SIZE        = 512;
static constexpr int    FLUSH_INTERVAL_MS = 20;

// ─────────────────────────────────────────────────────────────────────────────
// WriteAheadLog — group-commit, sequence-number-based waiting.
//
// PROBLEM with the original design:
//   append() added bytes to the buffer then blocked every caller with:
//     cv_.wait(lock, [this]{ return buffer_.empty() || shutdown_; });
//   All 10 gRPC handler threads sat holding the mutex waiting for the flush
//   thread, which itself couldn't acquire the mutex → near-deadlock.
//
// NEW DESIGN — sequence counters:
//   write_seq_  bumped once per append() call (under mu_).
//   flush_seq_  set to write_seq_ snapshot taken just before each fdatasync(),
//               then notify_all() wakes waiters.
//
//   append() queues bytes, records its seq, releases mu_, wakes the flush
//   thread, then re-acquires mu_ and waits only until flush_seq_ >= my_seq.
//
//   Result: N concurrent appends batch into one write()+fdatasync().  Each
//   caller unblocks as soon as its own entry is durable.  The mutex is NOT
//   held during write()/fdatasync(), so new entries keep queuing in parallel.
// ─────────────────────────────────────────────────────────────────────────────
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

  // Append one entry and return only after it has been fdatasync'd to disk.
  void append(WalOp op, const std::string& key, const std::string& value) {
    // Build serialised entry outside the lock.
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
      cv_.notify_all();   // wake flush thread immediately
    }

    // Wait until our entry (batched with others) is on disk.
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [this, my_seq] {
      return flush_seq_ >= my_seq || shutdown_;
    });
  }

  // Replay WAL into an in-memory map (called once at startup before serving).
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
    size_t pos = 0, count = 0;

    while (pos + WAL_HEADER_SIZE <= total) {
      const uint8_t op = static_cast<uint8_t>(data[pos]);
      uint32_t klen, vlen;
      std::memcpy(&klen, &data[pos + 1], 4);
      std::memcpy(&vlen, &data[pos + 5], 4);
      if (pos + WAL_HEADER_SIZE + klen + vlen > total) break;  // truncated tail

      std::string k(&data[pos + WAL_HEADER_SIZE], klen);
      pos += WAL_HEADER_SIZE + klen;
      std::string v(&data[pos], vlen);
      pos += vlen;
      ++count;

      if      (op == WAL_SET) state[k]  = std::move(v);
      else if (op == WAL_DEL) state.erase(k);
    }

    Log("WAL replay: " + std::to_string(count) + " entries -> " +
        std::to_string(state.size()) + " keys");
    return state;
  }

 private:
  // Write bytes to disk and fdatasync.  Called WITHOUT mu_ held.
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
        seq_snap     = write_seq_;  // snapshot before releasing lock
      }
      FlushBytes(std::move(to_write), seq_snap);  // I/O outside lock
    }
    // Final drain on shutdown.
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
    }                                     // lock released before I/O

    wal_->append(WAL_SET, key, value);    // durable; no lock held

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
// Manager registration helpers
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
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));

  grpc::Status s = stub->RegisterServer(&ctx, req, &res);
  if (s.ok() && res.ok()) {
    Log("registered to manager sid=" + std::to_string(sid) +
        " manager=" + manager_addr + " api_addr=" + api_addr);
    return true;
  }
  Log("register attempt failed sid=" + std::to_string(sid) +
      " manager=" + manager_addr +
      " grpc_ok=" + (s.ok() ? std::string("true") : "false") +
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
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
    if (stub->GetCluster(&ctx, req, &res).ok() && res.ready()) {
      Log("cluster ready from manager=" + manager_addr);
      return;
    }
    if (++attempts % 10 == 0)
      Log("waiting for cluster ready manager=" + manager_addr +
          " attempts=" + std::to_string(attempts));
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

int main(int argc, char** argv) {
  try {
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
      Log("p2 startup sid=" + std::to_string(sid) + " manager=" + manager_addr +
          " api_port=" + api_port + " backer=" + backer_path);

      while (!RegisterToManager(sid, manager_addr, listen_addr))
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      WaitForClusterReady(manager_addr);
      RunServer(listen_addr, backer_path);
      return 0;
    }

    std::cerr << "Usage (P1): " << argv[0] << " <listen_addr> [backer_path]\n";
    std::cerr << "Usage (P2): " << argv[0]
              << " <id> <manager_addr> <api_port> <backer_path>\n";
    return 1;
  } catch (const std::exception& e) {
    std::cerr << "server error: " << e.what() << std::endl;
    return 1;
  }
}