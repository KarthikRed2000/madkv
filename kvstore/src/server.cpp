#include <grpcpp/grpcpp.h>

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

class WriteAheadLog {
 public:
  explicit WriteAheadLog(const std::string& path) : path_(path) {
    fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0) {
      throw std::runtime_error("failed to open WAL: " + path);
    }
  }

  ~WriteAheadLog() {
    if (fd_ >= 0) ::close(fd_);
  }

  void append(WalOp op, const std::string& key, const std::string& value) {
    uint32_t klen = static_cast<uint32_t>(key.size());
    uint32_t vlen = static_cast<uint32_t>(value.size());

    buf_.clear();
    buf_.resize(WAL_HEADER_SIZE + klen + vlen);
    buf_[0] = static_cast<char>(op);
    std::memcpy(&buf_[1], &klen, 4);
    std::memcpy(&buf_[5], &vlen, 4);
    std::memcpy(&buf_[WAL_HEADER_SIZE], key.data(), klen);
    std::memcpy(&buf_[WAL_HEADER_SIZE + klen], value.data(), vlen);

    ssize_t w = ::write(fd_, buf_.data(), buf_.size());
    if (w != static_cast<ssize_t>(buf_.size())) {
      throw std::runtime_error("WAL write failed");
    }
    ::fdatasync(fd_);
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
      if (pos + entry_size > total) break;

      std::string key(&data[pos + WAL_HEADER_SIZE], klen);
      pos += WAL_HEADER_SIZE + klen;
      std::string value(&data[pos], vlen);
      pos += vlen;
      count++;

      if (op == WAL_SET) {
        state[key] = std::move(value);
      } else if (op == WAL_DEL) {
        state.erase(key);
      }
    }

    Log("WAL replay: " + std::to_string(count) + " entries -> " +
        std::to_string(state.size()) + " keys");
    return state;
  }

 private:
  std::string path_;
  int fd_ = -1;
  std::vector<char> buf_;
};

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
    const std::string& key = request->key();
    const std::string& value = request->value();

    std::unique_lock<std::shared_mutex> g(mu_);
    bool found = state_.count(key) > 0;
    wal_->append(WAL_SET, key, value);
    state_[key] = value;

    response->set_found(found);
    return Status::OK;
  }

  Status Swap(ServerContext* context, const SwapRequest* request,
              SwapResponse* response) override {
    (void)context;
    const std::string& key = request->key();
    const std::string& value = request->value();

    std::unique_lock<std::shared_mutex> g(mu_);
    auto it = state_.find(key);
    if (it != state_.end()) {
      response->set_found(true);
      response->set_old_value(it->second);
    } else {
      response->set_found(false);
    }
    wal_->append(WAL_SET, key, value);
    state_[key] = value;

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
    const std::string& end_key = request->end_key();

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
      wal_->append(WAL_DEL, key, "");
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
      std::string api_port = argv[3];
      std::string backer_path = argv[4];
      std::string listen_addr = "0.0.0.0:" + api_port;

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
