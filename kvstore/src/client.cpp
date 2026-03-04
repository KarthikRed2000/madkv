#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <fstream>
#include <chrono>

#include "kvstore.grpc.pb.h"

// #region agent log
void log_debug(const std::string& location, const std::string& message, const std::string& data, const std::string& hypothesisId) {
  std::ofstream logfile("/tmp/kvstore-debug.log", std::ios::app);
  auto now = std::chrono::system_clock::now().time_since_epoch();
  auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
  if (logfile.is_open()) {
    logfile << "{\"id\":\"log_" << millis << "\",\"timestamp\":" << millis << ",\"location\":\"" << location << "\",\"message\":\"" << message << "\",\"data\":" << data << ",\"hypothesisId\":\"" << hypothesisId << "\"}\n";
    logfile.close();
  }
}
// #endregion

using grpc::Channel;
using grpc::ClientContext;
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

class KVStoreClient {
 public:
  KVStoreClient(std::shared_ptr<Channel> channel)
      : stub_(KVStore::NewStub(channel)) {}

  void Put(const std::string& key, const std::string& value) {
    PutRequest request;
    request.set_key(key);
    request.set_value(value);

    PutResponse response;
    ClientContext context;

    Status status = stub_->Put(&context, request, &response);
    if (status.ok()) {
      std::cout << "PUT " << key << " "
                << (response.found() ? "found" : "not_found") << std::endl;
    } else {
      std::cerr << "RPC failed: " << status.error_message() << std::endl;
    }
  }

  void Swap(const std::string& key, const std::string& value) {
    SwapRequest request;
    request.set_key(key);
    request.set_value(value);

    SwapResponse response;
    ClientContext context;

    Status status = stub_->Swap(&context, request, &response);
    if (status.ok()) {
      // #region agent log
      std::string old_val = response.old_value();
      bool found = response.found();
      log_debug("client.cpp:60", "Swap response", "{\"key\":\"" + key + "\",\"old_value\":\"" + old_val + "\",\"found\":" + (found ? "true" : "false") + ",\"check_method\":\"found_field\"}", "C");
      // #endregion
      std::cout << "SWAP " << key << " ";
      if (response.found()) {
        std::cout << response.old_value() << std::endl;
      } else {
        std::cout << "null" << std::endl;
      }
    } else {
      std::cerr << "RPC failed: " << status.error_message() << std::endl;
    }
  }

  void Get(const std::string& key) {
    GetRequest request;
    request.set_key(key);

    GetResponse response;
    ClientContext context;

    Status status = stub_->Get(&context, request, &response);
    if (status.ok()) {
      // #region agent log
      std::string val = response.value();
      bool found = response.found();
      log_debug("client.cpp:84", "Get response", "{\"key\":\"" + key + "\",\"value\":\"" + val + "\",\"found\":" + (found ? "true" : "false") + ",\"check_method\":\"found_field\"}", "C");
      // #endregion
      std::cout << "GET " << key << " ";
      if (response.found()) {
        std::cout << response.value() << std::endl;
      } else {
        std::cout << "null" << std::endl;
      }
    } else {
      std::cerr << "RPC failed: " << status.error_message() << std::endl;
    }
  }

  void Scan(const std::string& start_key, const std::string& end_key) {
    ScanRequest request;
    request.set_start_key(start_key);
    request.set_end_key(end_key);

    ScanResponse response;
    ClientContext context;

    Status status = stub_->Scan(&context, request, &response);
    if (status.ok()) {
      // #region agent log
      std::string entries = "[";
      for (const auto& e : response.entries()) { entries += "\"" + e.key() + "\","; }
      if (!entries.empty() && entries.back() == ',') entries.pop_back();
      entries += "]";
      log_debug("client.cpp:108", "Scan response", "{\"start\":\"" + start_key + "\",\"end\":\"" + end_key + "\",\"count\":" + std::to_string(response.entries_size()) + ",\"keys\":" + entries + "}", "B");
      // #endregion
      std::cout << "SCAN " << start_key << " " << end_key << " BEGIN"
                << std::endl;
      for (const auto& entry : response.entries()) {
        std::cout << "  " << entry.key() << " " << entry.value() << std::endl;
      }
      std::cout << "SCAN END" << std::endl;
    } else {
      std::cerr << "RPC failed: " << status.error_message() << std::endl;
    }
  }

  void Delete(const std::string& key) {
    DeleteRequest request;
    request.set_key(key);

    DeleteResponse response;
    ClientContext context;

    Status status = stub_->Delete(&context, request, &response);
    if (status.ok()) {
      std::cout << "DELETE " << key << " "
                << (response.found() ? "found" : "not_found") << std::endl;
    } else {
      std::cerr << "RPC failed: " << status.error_message() << std::endl;
    }
  }

 private:
  std::unique_ptr<KVStore::Stub> stub_;
};

void ProcessCommands(KVStoreClient& client) {
  std::string line;
  while (std::getline(std::cin, line)) {
    // Skip empty lines
    if (line.empty() || line.find_first_not_of(" \t\n\r") == std::string::npos) {
      continue;
    }

    std::istringstream iss(line);
    std::string command;
    iss >> command;

    if (command == "PUT") {
      std::string key, value;
      iss >> key >> value;
      client.Put(key, value);
    } else if (command == "SWAP") {
      std::string key, value;
      iss >> key >> value;
      client.Swap(key, value);
    } else if (command == "GET") {
      std::string key;
      iss >> key;
      client.Get(key);
    } else if (command == "SCAN") {
      std::string start_key, end_key;
      iss >> start_key >> end_key;
      client.Scan(start_key, end_key);
    } else if (command == "DELETE") {
      std::string key;
      iss >> key;
      client.Delete(key);
    } else if (command == "STOP") {
      std::cout << "STOP" << std::endl;
      break;
    } else {
      std::cerr << "Unknown command: " << command << std::endl;
    }
  }
}

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <server_addr>" << std::endl;
    std::cerr << "Example: " << argv[0] << " 127.0.0.1:3777" << std::endl;
    return 1;
  }

  std::string server_address(argv[1]);

  // Create gRPC channel
  auto channel = grpc::CreateChannel(server_address,
                                     grpc::InsecureChannelCredentials());
  KVStoreClient client(channel);

  // Process stdin commands
  ProcessCommands(client);

  return 0;
}
