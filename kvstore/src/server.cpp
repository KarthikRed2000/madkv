#include <chrono>
#include <fstream>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>

#include "kvstore.grpc.pb.h"

// #region agent log
void log_debug(const std::string &location, const std::string &message,
               const std::string &data, const std::string &hypothesisId) {
  std::ofstream logfile("/tmp/kvstore-debug.log", std::ios::app);
  auto now = std::chrono::system_clock::now().time_since_epoch();
  auto millis =
      std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
  if (logfile.is_open()) {
    logfile << "{\"id\":\"log_" << millis << "\",\"timestamp\":" << millis
            << ",\"location\":\"" << location << "\",\"message\":\"" << message
            << "\",\"data\":" << data << ",\"hypothesisId\":\"" << hypothesisId
            << "\"}\n";
    logfile.close();
  }
}
// #endregion

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using kvstore::DeleteRequest;
using kvstore::DeleteResponse;
using kvstore::GetRequest;
using kvstore::GetResponse;
using kvstore::KVStore;
using kvstore::PutRequest;
using kvstore::PutResponse;
using kvstore::ScanRequest;
using kvstore::ScanResponse;
using kvstore::SwapRequest;
using kvstore::SwapResponse;

class KVStoreServiceImpl final : public KVStore::Service {
public:
  Status Put(ServerContext *context, const PutRequest *request,
             PutResponse *response) override {
    const std::string &key = request->key();
    const std::string &value = request->value();

    std::lock_guard<std::mutex> g(mu_);
    bool found = store_.count(key) > 0;
    store_[key] = value;

    response->set_found(found);
    return Status::OK;
  }

  Status Swap(ServerContext *context, const SwapRequest *request,
              SwapResponse *response) override {
    const std::string &key = request->key();
    const std::string &value = request->value();

    // #region agent log
    log_debug("server.cpp:45", "Swap entry",
              "{\"key\":\"" + key + "\",\"value\":\"" + value + "\"}", "C");
    // #endregion

    std::lock_guard<std::mutex> g(mu_);
    auto it = store_.find(key);
    if (it != store_.end()) {
      // #region agent log
      log_debug("server.cpp:51", "Swap found existing",
                "{\"key\":\"" + key + "\",\"old_value\":\"" + it->second +
                    "\",\"found_set\":true}",
                "C");
      // #endregion
      response->set_found(true);
      response->set_old_value(it->second);
      it->second = value;
    } else {
      // #region agent log
      log_debug("server.cpp:58", "Swap not found",
                "{\"key\":\"" + key + "\",\"found_set\":true}", "C");
      // #endregion
      response->set_found(false);
      store_[key] = value;
    }

    return Status::OK;
  }

  Status Get(ServerContext *context, const GetRequest *request,
             GetResponse *response) override {
    const std::string &key = request->key();

    std::lock_guard<std::mutex> g(mu_);
    auto it = store_.find(key);
    if (it != store_.end()) {
      // #region agent log
      log_debug("server.cpp:68", "Get found",
                "{\"key\":\"" + key + "\",\"value\":\"" + it->second +
                    "\",\"found_set\":true}",
                "C");
      // #endregion
      response->set_found(true);
      response->set_value(it->second);
    } else {
      // #region agent log
      log_debug("server.cpp:75", "Get not found",
                "{\"key\":\"" + key + "\",\"found_set\":true}", "C");
      // #endregion
      response->set_found(false);
    }
    return Status::OK;
  }

  Status Scan(ServerContext *context, const ScanRequest *request,
              ScanResponse *response) override {
    const std::string &start_key = request->start_key();
    const std::string &end_key = request->end_key();

    std::lock_guard<std::mutex> g(mu_);
    // #region agent log
    std::string store_keys = "[";
    for (auto &kv : store_) {
      store_keys += "\"" + kv.first + "\",";
    }
    if (store_keys.back() == ',')
      store_keys.pop_back();
    store_keys += "]";
    log_debug("server.cpp:84", "Scan range",
              "{\"start\":\"" + start_key + "\",\"end\":\"" + end_key +
                  "\",\"store_keys\":" + store_keys + "}",
              "B");
    // #endregion

    // Find range [start_key, end_key] inclusive
    auto start_it = store_.lower_bound(start_key);
    auto end_it = store_.upper_bound(end_key);

    for (auto it = start_it; it != end_it; ++it) {
      auto *entry = response->add_entries();
      entry->set_key(it->first);
      entry->set_value(it->second);
    }

    return Status::OK;
  }

  Status Delete(ServerContext *context, const DeleteRequest *request,
                DeleteResponse *response) override {
    const std::string &key = request->key();

    std::lock_guard<std::mutex> g(mu_);
    size_t erased = store_.erase(key);
    response->set_found(erased > 0);

    return Status::OK;
  }

private:
  // Using std::map for ordered keys (required for SCAN)
  // Using std::map for ordered keys (required for SCAN)
  std::map<std::string, std::string> store_;
  mutable std::mutex mu_;
};

void RunServer(const std::string &server_address) {
  KVStoreServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << " (linearizable)" << std::endl;

  server->Wait();
}

int main(int argc, char **argv) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <listen_addr>" << std::endl;
    std::cerr << "Example: " << argv[0] << " 0.0.0.0:3777" << std::endl;
    return 1;
  }

  std::string server_address(argv[1]);
  RunServer(server_address);

  return 0;
}
