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
  const auto t = std::chrono::system_clock::to_time_t(now);
  std::ostringstream oss;
  oss << std::put_time(std::localtime(&t), "%F %T");
  return oss.str();
}

static void Log(const std::string& msg) {
  std::cerr << "[" << NowTs() << "] [server] " << msg << std::endl;
}

// WAL entry: [op:1][klen:4][vlen:4][key_bytes][value_bytes]
enum WalOp : uint8_t { WAL_SET = 0, WAL_DEL = 1 };
static constexpr size_t WAL_HEADER_SIZE = 1 + 4 + 4;
static constexpr size_t BATCH_SIZE = 512;
static constexpr int FLUSH_INTERVAL_MS = 20;

// ─────────────────────────────────────────────────────────────────────────────
// WriteAheadLog
//
// FIX 1: Sequence-number-based waiting.
//
// Old design: every append() caller blocked on cv_.wait() until the *entire*
// buffer was empty.  With N concurrent gRPC threads all holding the mutex and
// sleeping on the CV, the flush thread could never acquire the mutex quickly →
// near-deadlock under load, killing throughput for concentrated workloads
// (e.g. bench 10 clients / 1 partition).
//
// New design:
//   • write_seq_  – monotonically increases by 1 each append().
//   • flush_seq_  – set to write_seq_ value captured before each fdatasync().
//   • Each append() caller records its own seq number, adds its bytes to the
//     shared buffer, signals the flush thread, then waits only until
//     flush_seq_ >= its own seq.  This means:
//       – Multiple concurrent appends batch their data together in one write/
//         fdatasync (good for throughput).
//       – Each caller unblocks as soon as ITS entry is durable, not when
//         everyone else's is (correct per-entry durability as required by spec).
//       – The mutex is NOT held during the write()/fdatasync() syscalls, so
//         other threads can queue their entries while I/O is in progress.
// ─────────────────────────────────────────────────────────────────────────────
class WriteAheadLog {
 public:
  explicit WriteAheadLog(const std::string& path) : path_(path) {
    fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0) {
      throw std::runtime_error("failed to open WAL: " + path);
    }
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

  // Append one WAL entry and return only after it has been fdatasync'd.
  // Multiple concurrent callers will batch their entries into a single flush.
  void append(WalOp op, const std::string& key, const std::string& value) {
    // Build the serialized entry outside the lock (no contention on memcpy).
    uint32_t klen = static_cast<uint32_t>(key.size());
    uint32_t vlen = static_cast<uint32_t>(value.size());
    std::vector<char> entry(WAL_HEADER_SIZE + klen + vlen);
    entry[0] = static_cast<char>(op);
    std::memcpy(&entry[1], &klen, 4);
    std::memcpy(&entry[5], &vlen, 4);
    std::memcpy(&entry[WAL_HEADER_SIZE], key.data(), klen);
    std::memcpy(&entry[WAL_HEADER_SIZE + klen], value.data(), vlen);

    uint64_t my_seq;
    {
      std::lock_guard<std::mutex> g(mu_);
      buffer_.insert(buffer_.end(), entry.begin(), entry.end());
      my_seq = ++write_seq_;
      // Wake the flush thread so it doesn't wait the full FLUSH_INTERVAL_MS.
      cv_.notify_all();
    }

    // Wait until our entry (and possibly a batch of others) has been flushed.
    // The flush thread will bump flush_seq_ to write_seq_ captured before each
    // fdatasync, then notify_all.
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [this, my_seq] {
      return flush_seq_ >= my_seq || shutdown_;
    });
  }

  static std::map<std::string, std::string> replay(const std::string& path) {
    std::map<std::string, std::string> state;
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return state;

    off_t file_size = ::lseek(fd, 0, SEEK_END);
    if (file_size <= 0) { ::close(fd); return state; }
    ::lseek(fd, 0, SEEK_SET);

    std::vector<char> data(static_cast<size_t>(file_size));
    ssize_t rd = ::read(fd, data.data(), data.size());
    ::close(fd);
    if (rd <= 0) return state;

    size_t total = static_cast<size_t>(rd);
    size_t pos = 0;
    size_t count = 0;

    while (pos + WAL_HEADER_SIZE <= total) {
      uint8_t op = static_cast<uint8_t>(data[pos]);
      uint32_t klen, vlen;
      std::memcpy(&klen, &data[pos + 1], 4);
      std::memcpy(&vlen, &data[pos + 5], 4);

      size_t entry_size = WAL_HEADER_SIZE + klen + vlen;
      if (pos + entry_size > total) break;  // truncated entry – discard tail

      std::string k(&data[pos + WAL_HEADER_SIZE], klen);
      pos += WAL_HEADER_SIZE + klen;
      std::string v(&data[pos], vlen);
      pos += vlen;
      count++;

      if (op == WAL_SET) {
        state[k] = std::move(v);
      } else if (op == WAL_DEL) {
        state.erase(k);
      }
    }

    Log("WAL replay: " + std::to_string(count) + " entries -> " +
        std::to_string(state.size()) + " keys");
    return state;
  }

 private:
  // Must be called with mu_ NOT held.  Drains buffer_, writes, fdatasyncs,
  // then bumps flush_seq_ to seq_before_write and notifies all waiters.
  void FlushBuffer(std::vector<char> to_write, uint64_t seq_before_write) {
    ssize_t w = ::write(fd_, to_write.data(), to_write.size());
    if (w != static_cast<ssize_t>(to_write.size())) {
      // Hard error – crash the server rather than silently lose durability.
      throw std::runtime_error("WAL write failed");
    }
#ifdef __APPLE__
    ::fsync(fd_);
#else
    ::fdatasync(fd_);
#endif
    // Update flush_seq_ and wake every caller whose entry is now durable.
    {
      std::lock_guard<std::mutex> g(mu_);
      if (seq_before_write > flush_seq_) {
        flush_seq_ = seq_before_write;
      }
      cv_.notify_all();
    }
  }

  void FlushLoop() {
    while (true) {
      std::vector<char> to_write;
      uint64_t seq_snapshot;
      {
        std::unique_lock<std::mutex> lock(mu_);
        // Wait up to FLUSH_INTERVAL_MS for new data or shutdown.
        cv_.wait_for(lock, std::chrono::milliseconds(FLUSH_INTERVAL_MS),
                     [this] { return !buffer_.empty() || shutdown_; });

        if (buffer_.empty()) {
          if (shutdown_) break;
          continue;
        }
        to_write   = std::move(buffer_);
        buffer_.clear();
        entry_count_ = 0;
        // Snapshot the write counter *before* releasing the lock so that any
        // entries added after this point will be flushed in a subsequent round.
        seq_snapshot = write_seq_;
      }
      // Perform I/O outside the lock so append() callers can keep queueing.
      FlushBuffer(std::move(to_write), seq_snapshot);
    }

    // Drain any final entries queued between the last flush and shutdown.
    std::vector<char> tail;
    uint64_t seq_snapshot;
    {
      std::lock_guard<std::mutex> g(mu_);
      tail         = std::move(buffer_);
      seq_snapshot = write_seq_;
    }
    if (!tail.empty()) {
      FlushBuffer(std::move(tail), seq_snapshot);
    }
  }

 private:
  std::string path_;
  int fd_ = -1;
  std::vector<char> buffer_;
  size_t entry_count_ = 0;

  // Sequence counters (guarded by mu_).
  uint64_t write_seq_ = 0;   // incremented by each append() call
  uint64_t flush_seq_ = 0;   // set to write_seq_ snapshot after each fdatasync

  std::mutex mu_;
  std::condition_variable cv_;
  std::thread flusher_;
  bool shutdown_ = false;
};

// ─────────────────────────────────────────────────────────────────────────────
// KVStoreServiceImpl
//
// FIX 2: Atomic read-modify-write in Put, Swap, and Delete.
//
// Old design: Put and Swap read the old state under a *shared* lock, dropped
// it, appended to the WAL (outside any lock), then re-acquired an exclusive
// lock to update the in-memory map.  Concurrent writers could interleave
// between the read and write phases, causing stale `found`/`old_value` results
// and incorrect linearizability under conflicting workloads (the fuzz tester
// exercises exactly this).
//
// New design: Put, Swap, and Delete each hold a single *exclusive* lock for
// the entire read + WAL-append + map-write sequence, eliminating TOCTOU races.
// Get and Scan remain under shared locks (read-only, no race possible).
//
// Note on lock ordering: wal_->append() blocks until the entry is durable, so
// we hold the exclusive state lock while waiting for disk I/O.  This is safe
// for correctness (no deadlock possible – the WAL has no back-reference to the
// state lock) and is the standard approach for a single-node write-ahead log.
// ─────────────────────────────────────────────────────────────────────────────
class KVStoreServiceImpl final : public KVStore::Service {
 public:
  explicit KVStoreServiceImpl(const std::string& db_path) : db_path_(db_path) {
    std::filesystem::create_directories(db_path);
    std::string wal_path = db_path + "/wal.log";

    state_ = WriteAheadLog::replay(wal_path);
    wal_ = std::make_unique<WriteAheadLog>(wal_path);
    Log("state machine ready path=" + db_path_ +
        " keys=" + std::to_string(state_.size()));
  }

  Status Put(ServerContext* context, const PutRequest* request,
             PutResponse* response) override {
    (void)context;
    const std::string& key   = request->key();
    const std::string& value = request->value();

    // Hold exclusive lock for the full read-WAL-write sequence.
    std::unique_lock<std::shared_mutex> g(mu_);
    bool found = state_.count(key) > 0;
    wal_->append(WAL_SET, key, value);   // durable before ack
    state_[key] = value;
    response->set_found(found);
    return Status::OK;
  }

  Status Swap(ServerContext* context, const SwapRequest* request,
              SwapResponse* response) override {
    (void)context;
    const std::string& key   = request->key();
    const std::string& value = request->value();

    std::unique_lock<std::shared_mutex> g(mu_);
    auto it = state_.find(key);
    bool found = (it != state_.end());
    std::string old_value = found ? it->second : "";

    wal_->append(WAL_SET, key, value);   // durable before ack
    state_[key] = value;

    response->set_found(found);
    if (found) response->set_old_value(old_value);
    return Status::OK;
  }

  Status Get(ServerContext* context, const GetRequest* request,
             GetResponse* response) override {
    (void)context;
    const std::string& key = request->key();

    std::shared_lock<std::shared_mutex> g(mu_);
    auto it = state_.find(key);
    if (it != state_.end()) {
      response->set_found(true);
      response->set_value(it->second);
    } else {
      response->set_found(false);
    }
    return Status::OK;
  }

  Status Scan(ServerContext* context, const ScanRequest* request,
              ScanResponse* response) override {
    (void)context;
    const std::string& start_key = request->start_key();
    const std::string& end_key   = request->end_key();

    std::shared_lock<std::shared_mutex> g(mu_);
    for (auto it = state_.lower_bound(start_key);
         it != state_.end() && it->first <= end_key; ++it) {
      auto* entry = response->add_entries();
      entry->set_key(it->first);
      entry->set_value(it->second);
    }
    return Status::OK;
  }

  Status Delete(ServerContext* context, const DeleteRequest* request,
                DeleteResponse* response) override {
    (void)context;
    const std::string& key = request->key();

    std::unique_lock<std::shared_mutex> g(mu_);
    auto it = state_.find(key);
    bool found = (it != state_.end());
    if (found) {
      wal_->append(WAL_DEL, key, "");  // durable before ack
      state_.erase(it);
    }
    response->set_found(found);
    return Status::OK;
  }

 private:
  std::string db_path_;
  std::map<std::string, std::string> state_;
  std::unique_ptr<WriteAheadLog> wal_;
  mutable std::shared_mutex mu_;
};

static bool RegisterToManager(uint32_t sid, const std::string& manager_addr,
                              const std::string& api_addr) {
  auto channel = grpc::CreateChannel(manager_addr, grpc::InsecureChannelCredentials());
  auto stub = ClusterManager::NewStub(channel);

  RegisterServerRequest req;
  req.set_server_id(sid);
  req.set_api_addr(api_addr);
  RegisterServerResponse res;
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));

  grpc::Status s = stub->RegisterServer(&ctx, req, &res);
  if (s.ok() && res.ok()) {
    Log("registered to manager sid=" + std::to_string(sid) + " manager=" +
        manager_addr + " api_addr=" + api_addr);
    return true;
  }
  Log("register attempt failed sid=" + std::to_string(sid) +
      " manager=" + manager_addr +
      " grpc_ok=" + std::string(s.ok() ? "true" : "false") +
      (s.ok() ? "" : " err=" + s.error_message()));
  return false;
}

static void WaitForClusterReady(const std::string& manager_addr) {
  auto channel = grpc::CreateChannel(manager_addr, grpc::InsecureChannelCredentials());
  auto stub = ClusterManager::NewStub(channel);
  int attempts = 0;
  while (true) {
    GetClusterRequest req;
    GetClusterResponse res;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
    grpc::Status s = stub->GetCluster(&ctx, req, &res);
    if (s.ok() && res.ready()) {
      Log("cluster ready from manager=" + manager_addr);
      return;
    }
    attempts++;
    if (attempts % 10 == 0) {
      Log("waiting for cluster ready manager=" + manager_addr +
          " attempts=" + std::to_string(attempts));
    }
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
    if (argc == 2 || argc == 3) {
      std::string listen_addr = argv[1];
      std::string backer_path = (argc == 3) ? argv[2] : "./backer.default";
      std::filesystem::create_directories(backer_path);
      RunServer(listen_addr, backer_path);
      return 0;
    }

    if (argc == 5) {
      uint32_t sid = static_cast<uint32_t>(std::stoul(argv[1]));
      std::string manager_addr = argv[2];
      std::string api_port     = argv[3];
      std::string backer_path  = argv[4];
      std::string listen_addr  = "0.0.0.0:" + api_port;

      std::filesystem::create_directories(backer_path);
      Log("p2 startup sid=" + std::to_string(sid) + " manager=" + manager_addr +
          " api_port=" + api_port + " backer=" + backer_path);

      while (!RegisterToManager(sid, manager_addr, listen_addr)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      }
      WaitForClusterReady(manager_addr);
      RunServer(listen_addr, backer_path);
      return 0;
    }

    std::cerr << "Usage (P1): " << argv[0] << " <listen_addr> [backer_path]"
              << std::endl;
    std::cerr << "Usage (P2): " << argv[0]
              << " <id> <manager_addr> <api_port> <backer_path>" << std::endl;
    return 1;
  } catch (const std::exception& e) {
    std::cerr << "server error: " << e.what() << std::endl;
    return 1;
  }
}