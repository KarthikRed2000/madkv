#include <grpcpp/grpcpp.h>

#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include "cluster.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using kvstore::ClusterManager;
using kvstore::GetClusterRequest;
using kvstore::GetClusterResponse;
using kvstore::PartitionInfo;
using kvstore::RegisterServerRequest;
using kvstore::RegisterServerResponse;

static std::vector<std::string> SplitCsv(const std::string& csv) {
  std::vector<std::string> out;
  std::stringstream ss(csv);
  std::string item;
  while (std::getline(ss, item, ',')) {
    if (!item.empty()) {
      out.push_back(item);
    }
  }
  return out;
}

class ClusterManagerService final : public ClusterManager::Service {
 public:
  explicit ClusterManagerService(std::vector<std::string> expected_servers)
      : expected_servers_(std::move(expected_servers)),
        registered_(expected_servers_.size(), false) {}

  Status RegisterServer(ServerContext* context, const RegisterServerRequest* request,
                        RegisterServerResponse* response) override {
    (void)context;
    std::lock_guard<std::mutex> lock(mu_);
    const uint32_t sid = request->server_id();
    if (sid >= expected_servers_.size()) {
      response->set_ok(false);
      response->set_error("invalid server id");
      return Status::OK;
    }

    // Track the live advertised address so clients can connect to restarted nodes.
    if (!request->api_addr().empty()) {
      expected_servers_[sid] = request->api_addr();
    }

    registered_[sid] = true;
    response->set_ok(true);
    response->set_partition_id(sid);
    response->set_num_partitions(static_cast<uint32_t>(expected_servers_.size()));
    return Status::OK;
  }

  Status GetCluster(ServerContext* context, const GetClusterRequest* request,
                    GetClusterResponse* response) override {
    (void)context;
    (void)request;
    std::lock_guard<std::mutex> lock(mu_);

    bool ready = true;
    for (bool r : registered_) {
      if (!r) {
        ready = false;
        break;
      }
    }
    response->set_ready(ready);

    for (uint32_t i = 0; i < expected_servers_.size(); ++i) {
      PartitionInfo* p = response->add_partitions();
      p->set_partition_id(i);
      p->set_server_id(i);
      p->set_api_addr(expected_servers_[i]);
      p->set_registered(registered_[i]);
    }
    return Status::OK;
  }

 private:
  std::mutex mu_;
  std::vector<std::string> expected_servers_;
  std::vector<bool> registered_;
};

static void RunManager(const std::string& listen_addr, const std::string& servers_csv) {
  std::vector<std::string> expected_servers = SplitCsv(servers_csv);
  if (expected_servers.empty()) {
    throw std::runtime_error("servers csv is empty");
  }

  ClusterManagerService service(expected_servers);

  ServerBuilder builder;
  builder.AddListeningPort(listen_addr, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Manager listening on " << listen_addr << " with "
            << expected_servers.size() << " partitions" << std::endl;
  server->Wait();
}

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <listen_addr> <servers_csv>" << std::endl;
    std::cerr << "Example: " << argv[0]
              << " 0.0.0.0:3666 127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779"
              << std::endl;
    return 1;
  }

  try {
    RunManager(argv[1], argv[2]);
  } catch (const std::exception& e) {
    std::cerr << "manager error: " << e.what() << std::endl;
    return 1;
  }
  return 0;
}
