#include <grpcpp/grpcpp.h>

#include <cstdint>
#include <ctime>
#include <iomanip>
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

static std::string NowTs() {
  const auto now = std::chrono::system_clock::now();
  const auto t = std::chrono::system_clock::to_time_t(now);
  std::ostringstream oss;
  oss << std::put_time(std::localtime(&t), "%F %T");
  return oss.str();
}

static void Log(const std::string& msg) {
  std::cerr << "[" << NowTs() << "] [manager] " << msg << std::endl;
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
// P2 single-manager service
// ─────────────────────────────────────────────────────────────────────────────
class ClusterManagerService final : public ClusterManager::Service {
 public:
  explicit ClusterManagerService(std::vector<std::string> expected_servers)
      : expected_servers_(std::move(expected_servers)),
        registered_(expected_servers_.size(), false) {
    Log("expected server endpoints count=" +
        std::to_string(expected_servers_.size()));
  }

  Status RegisterServer(ServerContext* context, const RegisterServerRequest* request,
                        RegisterServerResponse* response) override {
    (void)context;
    std::lock_guard<std::mutex> lock(mu_);
    const uint32_t sid = request->server_id();
    if (sid >= expected_servers_.size()) {
      Log("register rejected sid=" + std::to_string(sid) + " reason=invalid_id");
      response->set_ok(false);
      response->set_error("invalid server id");
      return Status::OK;
    }
    registered_[sid] = true;
    Log("server registered sid=" + std::to_string(sid) +
        " api_addr=" + request->api_addr() +
        " mapped_to=" + expected_servers_[sid]);
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
    for (bool r : registered_) if (!r) { ready = false; break; }
    response->set_ready(ready);
    if (!has_last_ready_ || ready != last_ready_) {
      Log(std::string("cluster ready state: ") + (ready ? "ready" : "not_ready"));
      has_last_ready_ = true;
      last_ready_ = ready;
    }
    for (uint32_t i = 0; i < expected_servers_.size(); ++i) {
      PartitionInfo* p = response->add_partitions();
      p->set_partition_id(i);
      p->set_server_id(i);
      p->set_api_addr(expected_servers_[i]);
      p->set_registered(registered_[i]);
      p->add_replica_addrs(expected_servers_[i]);  // single replica
    }
    return Status::OK;
  }

 private:
  std::mutex mu_;
  std::vector<std::string> expected_servers_;
  std::vector<bool> registered_;
  bool has_last_ready_ = false;
  bool last_ready_ = false;
};

// ─────────────────────────────────────────────────────────────────────────────
// P3 multi-replica manager service
//
// server_addrs layout: [p0r0, p0r1, ..., p0r(rf-1), p1r0, p1r1, ...]
// server_id = partition_id * server_rf + replica_id
// ─────────────────────────────────────────────────────────────────────────────
class P3ClusterManagerService final : public ClusterManager::Service {
 public:
  P3ClusterManagerService(int server_rf, std::vector<std::string> server_addrs)
      : server_rf_(server_rf),
        server_addrs_(std::move(server_addrs)),
        registered_(server_addrs_.size(), false) {
    num_partitions_ = static_cast<uint32_t>(server_addrs_.size() / server_rf_);
    Log("P3 manager: partitions=" + std::to_string(num_partitions_) +
        " rf=" + std::to_string(server_rf_) +
        " total_servers=" + std::to_string(server_addrs_.size()));
  }

  Status RegisterServer(ServerContext* context, const RegisterServerRequest* request,
                        RegisterServerResponse* response) override {
    (void)context;
    std::lock_guard<std::mutex> lock(mu_);
    uint32_t sid = request->server_id();
    if (sid >= server_addrs_.size()) {
      response->set_ok(false);
      response->set_error("invalid server id " + std::to_string(sid));
      return Status::OK;
    }
    registered_[sid] = true;
    uint32_t part_id  = sid / server_rf_;
    uint32_t rep_id   = sid % server_rf_;
    Log("registered sid=" + std::to_string(sid) +
        " partition=" + std::to_string(part_id) +
        " replica=" + std::to_string(rep_id) +
        " api_addr=" + request->api_addr() +
        " mapped_to=" + server_addrs_[sid]);
    response->set_ok(true);
    response->set_partition_id(part_id);
    response->set_num_partitions(num_partitions_);
    return Status::OK;
  }

  Status GetCluster(ServerContext* context, const GetClusterRequest* request,
                    GetClusterResponse* response) override {
    (void)context;
    (void)request;
    std::lock_guard<std::mutex> lock(mu_);

    bool ready = true;
    for (bool r : registered_) if (!r) { ready = false; break; }
    response->set_ready(ready);

    if (!has_last_ready_ || ready != last_ready_) {
      Log(std::string("cluster ready state: ") + (ready ? "ready" : "not_ready"));
      has_last_ready_ = true;
      last_ready_     = ready;
    }

    for (uint32_t p = 0; p < num_partitions_; p++) {
      PartitionInfo* pi = response->add_partitions();
      pi->set_partition_id(p);
      pi->set_server_id(p * server_rf_);
      pi->set_api_addr(server_addrs_[p * server_rf_]);  // first replica (P2 compat)
      pi->set_registered(registered_[p * server_rf_]);
      for (int r = 0; r < server_rf_; r++) {
        pi->add_replica_addrs(server_addrs_[p * server_rf_ + r]);
      }
    }
    return Status::OK;
  }

 private:
  std::mutex   mu_;
  int          server_rf_;
  uint32_t     num_partitions_ = 0;
  std::vector<std::string> server_addrs_;
  std::vector<bool>        registered_;
  bool has_last_ready_ = false;
  bool last_ready_     = false;
};

// ─────────────────────────────────────────────────────────────────────────────
// P2 runner
// ─────────────────────────────────────────────────────────────────────────────
static void RunManager(const std::string& listen_addr, const std::string& servers_csv) {
  std::vector<std::string> expected_servers = SplitCsv(servers_csv);
  if (expected_servers.empty())
    throw std::runtime_error("servers csv is empty");
  Log("startup listen_addr=" + listen_addr + " servers_csv=" + servers_csv);
  ClusterManagerService service(expected_servers);
  ServerBuilder builder;
  builder.AddListeningPort(listen_addr, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Manager listening on " << listen_addr << " with "
            << expected_servers.size() << " partitions" << std::endl;
  server->Wait();
}

// ─────────────────────────────────────────────────────────────────────────────
// P3 args and runner
// ─────────────────────────────────────────────────────────────────────────────
struct P3ManagerArgs {
  int                      replica_id   = 0;
  std::string              man_port;
  std::string              p2p_port;
  std::vector<std::string> peer_addrs;   // other manager P2P addrs (or empty)
  int                      server_rf    = 1;
  std::vector<std::string> server_addrs;
  std::string              backer_path;
};

static P3ManagerArgs ParseP3ManagerArgs(int argc, char** argv) {
  P3ManagerArgs args;
  for (int i = 1; i < argc; i++) {
    std::string flag = argv[i];
    if      (flag == "--replica_id")   args.replica_id   = std::stoi(argv[++i]);
    else if (flag == "--man_port")     args.man_port     = argv[++i];
    else if (flag == "--p2p_port")     args.p2p_port     = argv[++i];
    else if (flag == "--peer_addrs") {
      std::string s = argv[++i];
      if (s != "none") args.peer_addrs = SplitCsv(s);
    }
    else if (flag == "--server_rf")    args.server_rf    = std::stoi(argv[++i]);
    else if (flag == "--server_addrs") args.server_addrs = SplitCsv(argv[++i]);
    else if (flag == "--backer_path")  args.backer_path  = argv[++i];
  }
  return args;
}

static void RunP3Manager(const P3ManagerArgs& args) {
  if (args.server_addrs.empty())
    throw std::runtime_error("--server_addrs is required");

  std::string listen_addr = "0.0.0.0:" + args.man_port;
  Log("P3 manager startup replica_id=" + std::to_string(args.replica_id) +
      " listen=" + listen_addr +
      " server_rf=" + std::to_string(args.server_rf) +
      " num_server_addrs=" + std::to_string(args.server_addrs.size()));

  P3ClusterManagerService service(args.server_rf, args.server_addrs);
  ServerBuilder builder;
  builder.AddListeningPort(listen_addr, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "P3 Manager listening on " << listen_addr << std::endl;
  server->Wait();
}

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────
int main(int argc, char** argv) {
  try {
    // P3 mode: first arg is a flag starting with --
    if (argc >= 2 && std::string(argv[1]).rfind("--", 0) == 0) {
      P3ManagerArgs args = ParseP3ManagerArgs(argc, argv);
      RunP3Manager(args);
      return 0;
    }

    // P2 mode: <listen_addr> <servers_csv>
    if (argc == 3) {
      RunManager(argv[1], argv[2]);
      return 0;
    }

    std::cerr << "Usage (P2): " << argv[0] << " <listen_addr> <servers_csv>\n";
    std::cerr << "Usage (P3): " << argv[0]
              << " --replica_id N --man_port P --p2p_port P\n"
                 "             --peer_addrs none|a:p,...\n"
                 "             --server_rf N --server_addrs a:p,...\n"
                 "             --backer_path PATH\n";
    return 1;
  } catch (const std::exception& e) {
    std::cerr << "manager error: " << e.what() << std::endl;
    return 1;
  }
}
