#include <grpcpp/grpcpp.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include <chrono>
#include <cstdint>
#include <ctime>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>

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

class KVStoreServiceImpl final : public KVStore::Service {
 public:
  explicit KVStoreServiceImpl(const std::string& db_path) : db_path_(db_path) {
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status s = rocksdb::DB::Open(options, db_path, &db_);
    if (!s.ok()) {
      throw std::runtime_error("failed to open RocksDB at " + db_path + ": " +
                               s.ToString());
    }
    Log("rocksdb opened path=" + db_path_);
  }

  ~KVStoreServiceImpl() override { delete db_; }

  Status Put(ServerContext* context, const PutRequest* request,
             PutResponse* response) override {
    (void)context;
    const std::string& key = request->key();
    const std::string& value = request->value();

    std::lock_guard<std::mutex> g(mu_);
    std::string old_value;
    rocksdb::Status get_s = db_->Get(rocksdb::ReadOptions(), key, &old_value);
    bool found = get_s.ok();
    if (!found && !get_s.IsNotFound()) {
      Log("Put read error key=" + key + " status=" + get_s.ToString());
      return Status(grpc::StatusCode::INTERNAL, get_s.ToString());
    }

    rocksdb::WriteOptions write_opts;
    write_opts.sync = true;
    rocksdb::Status put_s = db_->Put(write_opts, key, value);
    if (!put_s.ok()) {
      Log("Put write error key=" + key + " status=" + put_s.ToString());
      return Status(grpc::StatusCode::INTERNAL, put_s.ToString());
    }

    response->set_found(found);
    return Status::OK;
  }

  Status Swap(ServerContext* context, const SwapRequest* request,
              SwapResponse* response) override {
    (void)context;
    const std::string& key = request->key();
    const std::string& value = request->value();

    std::lock_guard<std::mutex> g(mu_);
    std::string old_value;
    rocksdb::Status get_s = db_->Get(rocksdb::ReadOptions(), key, &old_value);
    if (get_s.ok()) {
      response->set_found(true);
      response->set_old_value(old_value);
    } else if (get_s.IsNotFound()) {
      response->set_found(false);
    } else {
      Log("Swap read error key=" + key + " status=" + get_s.ToString());
      return Status(grpc::StatusCode::INTERNAL, get_s.ToString());
    }

    rocksdb::WriteOptions write_opts;
    write_opts.sync = true;
    rocksdb::Status put_s = db_->Put(write_opts, key, value);
    if (!put_s.ok()) {
      Log("Swap write error key=" + key + " status=" + put_s.ToString());
      return Status(grpc::StatusCode::INTERNAL, put_s.ToString());
    }
    return Status::OK;
  }

  Status Get(ServerContext* context, const GetRequest* request,
             GetResponse* response) override {
    (void)context;
    const std::string& key = request->key();
    std::lock_guard<std::mutex> g(mu_);
    std::string value;
    rocksdb::Status get_s = db_->Get(rocksdb::ReadOptions(), key, &value);
    if (get_s.ok()) {
      response->set_found(true);
      response->set_value(value);
    } else if (get_s.IsNotFound()) {
      response->set_found(false);
    } else {
      Log("Get read error key=" + key + " status=" + get_s.ToString());
      return Status(grpc::StatusCode::INTERNAL, get_s.ToString());
    }
    return Status::OK;
  }

  Status Scan(ServerContext* context, const ScanRequest* request,
              ScanResponse* response) override {
    (void)context;
    const std::string& start_key = request->start_key();
    const std::string& end_key = request->end_key();

    std::lock_guard<std::mutex> g(mu_);
    auto it =
        std::unique_ptr<rocksdb::Iterator>(db_->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(start_key); it->Valid() && it->key().ToString() <= end_key;
         it->Next()) {
      auto* entry = response->add_entries();
      entry->set_key(it->key().ToString());
      entry->set_value(it->value().ToString());
    }
    if (!it->status().ok()) {
      Log("Scan iterator error start=" + start_key + " end=" + end_key +
          " status=" + it->status().ToString());
      return Status(grpc::StatusCode::INTERNAL, it->status().ToString());
    }
    return Status::OK;
  }

  Status Delete(ServerContext* context, const DeleteRequest* request,
                DeleteResponse* response) override {
    (void)context;
    const std::string& key = request->key();

    std::lock_guard<std::mutex> g(mu_);
    std::string old_value;
    rocksdb::Status get_s = db_->Get(rocksdb::ReadOptions(), key, &old_value);
    if (!get_s.ok() && !get_s.IsNotFound()) {
      Log("Delete read error key=" + key + " status=" + get_s.ToString());
      return Status(grpc::StatusCode::INTERNAL, get_s.ToString());
    }
    bool found = get_s.ok();
    if (found) {
      rocksdb::WriteOptions write_opts;
      write_opts.sync = true;
      rocksdb::Status del_s = db_->Delete(write_opts, key);
      if (!del_s.ok()) {
        Log("Delete write error key=" + key + " status=" + del_s.ToString());
        return Status(grpc::StatusCode::INTERNAL, del_s.ToString());
      }
    }
    response->set_found(found);
    return Status::OK;
  }

 private:
  std::string db_path_;
  rocksdb::DB* db_ = nullptr;
  mutable std::mutex mu_;
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
    // Project 1 mode: kvserver <listen_addr> [backer_path]
    if (argc == 2 || argc == 3) {
      std::string listen_addr = argv[1];
      std::string backer_path = (argc == 3) ? argv[2] : "./backer.default";
      std::filesystem::create_directories(backer_path);
      RunServer(listen_addr, backer_path);
      return 0;
    }

    // Project 2 mode: kvserver <id> <manager_addr> <api_port> <backer_path>
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
