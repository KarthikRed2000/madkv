// raft.h — Single-header Raft consensus implementation for MadKV P3.
//
// Design summary:
//   • Standard Raft: leader election (randomised 2–4 s timeouts), log
//     replication, commit advancement, and durable state.
//   • Three background threads: ElectionLoop, ReplicationLoop, ApplyLoop.
//   • All Raft state is protected by a single mutex (mu_).  No I/O is
//     performed while mu_ is held (except SaveMeta/SaveFullLog on the
//     critical path; acceptable for a course project).
//   • Writes go through the log; reads are served by the leader from its
//     in-memory state (no read-log round-trip).
//   • Non-leaders return FAILED_PRECONDITION("NOT_LEADER:<api_addr>").
//   • Persistence: backer_path/raft_meta + backer_path/raft_log.
#pragma once

#include <grpcpp/grpcpp.h>
#include "raft.grpc.pb.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <functional>
#include <future>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

// ─────────────────────────────────────────────────────────────────────────────
// KV command serialisation
// ─────────────────────────────────────────────────────────────────────────────

enum class KVCmdType : uint8_t { PUT = 0, SWAP = 1, DEL = 2, NOOP = 3 };

struct KVCmd {
  KVCmdType   type  = KVCmdType::PUT;
  std::string key;
  std::string value;

  std::string Serialize() const {
    uint32_t klen = static_cast<uint32_t>(key.size());
    uint32_t vlen = static_cast<uint32_t>(value.size());
    std::string out(1 + 4 + 4 + klen + vlen, '\0');
    out[0] = static_cast<char>(type);
    std::memcpy(&out[1], &klen, 4);
    std::memcpy(&out[5], &vlen, 4);
    std::memcpy(&out[9],        key.data(),   klen);
    std::memcpy(&out[9 + klen], value.data(), vlen);
    return out;
  }

  static KVCmd Deserialize(const std::string& bytes) {
    KVCmd cmd;
    cmd.type = static_cast<KVCmdType>(static_cast<uint8_t>(bytes[0]));
    uint32_t klen = 0, vlen = 0;
    std::memcpy(&klen, &bytes[1], 4);
    std::memcpy(&vlen, &bytes[5], 4);
    cmd.key   = bytes.substr(9,        klen);
    cmd.value = bytes.substr(9 + klen, vlen);
    return cmd;
  }
};

struct KVResult {
  bool        found     = false;
  std::string old_value;  // populated for SWAP
};

// ─────────────────────────────────────────────────────────────────────────────
// NotLeaderError — thrown by Submit() on non-leader nodes
// ─────────────────────────────────────────────────────────────────────────────

struct NotLeaderError : std::exception {
  std::string leader_api_addr;
  std::string msg;
  explicit NotLeaderError(std::string addr)
      : leader_api_addr(std::move(addr)),
        msg("NOT_LEADER:" + leader_api_addr) {}
  const char* what() const noexcept override { return msg.c_str(); }
};

// ─────────────────────────────────────────────────────────────────────────────
// Internal log entry
// ─────────────────────────────────────────────────────────────────────────────

struct RaftEntry {
  int64_t     term     = 0;
  std::string cmd_bytes;
};

// ─────────────────────────────────────────────────────────────────────────────
// RaftNode
// ─────────────────────────────────────────────────────────────────────────────

class RaftNode {
 public:
  // id          : 0-based replica ID of this node
  // all_api_addrs : API addresses of every replica, indexed by replica ID
  // all_p2p_addrs : P2P addresses of every replica, indexed by replica ID
  //                 (the slot for `id` may be an empty string or "0.0.0.0:…")
  // backer_path : directory for durable storage
  // apply_fn    : called (outside mu_) for each committed log entry
  RaftNode(int id,
           std::vector<std::string> all_api_addrs,
           std::vector<std::string> all_p2p_addrs,
           std::string              backer_path,
           std::function<KVResult(const KVCmd&)> apply_fn)
      : my_id_(id),
        rf_(static_cast<int>(all_api_addrs.size())),
        all_api_addrs_(std::move(all_api_addrs)),
        all_p2p_addrs_(std::move(all_p2p_addrs)),
        backer_path_(std::move(backer_path)),
        apply_fn_(std::move(apply_fn)),
        rng_(std::random_device{}()) {
    // gRPC stubs to peers
    peer_stubs_.resize(rf_);
    for (int i = 0; i < rf_; i++) {
      if (i == my_id_ || all_p2p_addrs_[i].empty()) continue;
      auto ch = grpc::CreateChannel(all_p2p_addrs_[i],
                                    grpc::InsecureChannelCredentials());
      peer_stubs_[i] = raft::RaftService::NewStub(ch);
    }

    std::filesystem::create_directories(backer_path_);

    // Sentinel at index 0 — must be present before LoadState appends disk entries,
    // otherwise all indices are off by 1 after a restart.
    log_.push_back({0, ""});
    LoadState();

    // Replay committed entries to rebuild volatile state
    for (int64_t i = 1; i <= commit_index_ && i < (int64_t)log_.size(); i++) {
      KVCmd cmd = KVCmd::Deserialize(log_[i].cmd_bytes);
      apply_fn_(cmd);
    }
    last_applied_ = commit_index_;
  }

  ~RaftNode() { Stop(); }

  void Start() {
    stopped_ = false;
    last_heartbeat_received_ = std::chrono::steady_clock::now();

    // Single-node cluster: become leader immediately
    if (rf_ == 1) {
      std::lock_guard<std::mutex> g(mu_);
      BecomeLeader();
    }

    election_thread_     = std::thread(&RaftNode::ElectionLoop,     this);
    replication_thread_  = std::thread(&RaftNode::ReplicationLoop,  this);
    apply_thread_        = std::thread(&RaftNode::ApplyLoop,        this);
  }

  void Stop() {
    bool expected = false;
    if (!stopped_.compare_exchange_strong(expected, true)) return;

    apply_cv_.notify_all();
    repl_cv_.notify_all();
    elect_cv_.notify_all();

    if (election_thread_.joinable())    election_thread_.join();
    if (replication_thread_.joinable()) replication_thread_.join();
    if (apply_thread_.joinable())       apply_thread_.join();
  }

  // Submit a write command (leader only). Blocks until committed+applied.
  // Throws NotLeaderError if this node is not the leader.
  KVResult Submit(const KVCmd& cmd) {
    std::promise<KVResult> promise;
    std::future<KVResult>  fut = promise.get_future();
    int64_t my_index;

    {
      std::lock_guard<std::mutex> g(mu_);
      if (role_ != Role::LEADER) {
        std::string hint = LeaderApiAddrLocked();
        throw NotLeaderError(hint);
      }
      RaftEntry entry{current_term_, cmd.Serialize()};
      AppendLogEntryLocked(entry);
      my_index = LastLogIndex();
      pending_[my_index] = std::move(promise);
      // Update leader's own match index for commit counting.
      // Also advance commit index here so RF=1 clusters commit immediately
      // (ReplicateToPeer is never called when there are no peers).
      match_index_[my_id_] = my_index;
      AdvanceCommitIndex();
    }

    // Wake replication loop to send immediately
    repl_cv_.notify_all();

    // Wait (with generous timeout)
    if (fut.wait_for(std::chrono::seconds(120)) == std::future_status::timeout) {
      std::lock_guard<std::mutex> g(mu_);
      pending_.erase(my_index);
      throw std::runtime_error("raft: submit timed out");
    }
    return fut.get();
  }

  bool IsLeader() const {
    std::lock_guard<std::mutex> g(mu_);
    return role_ == Role::LEADER;
  }

  std::string GetLeaderApiAddr() const {
    std::lock_guard<std::mutex> g(mu_);
    return LeaderApiAddrLocked();
  }

  // ── RPC handlers (called by RaftServiceImpl via gRPC threads) ────────────

  void HandleRequestVote(const raft::RequestVoteRequest*  req,
                               raft::RequestVoteResponse* res) {
    std::lock_guard<std::mutex> g(mu_);

    if (req->term() < current_term_) {
      res->set_term(current_term_);
      res->set_vote_granted(false);
      return;
    }

    if (req->term() > current_term_) BecomeFollower(req->term());

    bool log_ok =
        (req->last_log_term() > LastLogTerm()) ||
        (req->last_log_term() == LastLogTerm() &&
         req->last_log_index() >= LastLogIndex());

    bool can_vote =
        (voted_for_ == -1 || voted_for_ == req->candidate_id()) && log_ok;

    if (can_vote) {
      voted_for_               = req->candidate_id();
      last_heartbeat_received_ = std::chrono::steady_clock::now();
      SaveMeta();
      RaftLog("granted vote to " + std::to_string(req->candidate_id()) +
              " term=" + std::to_string(current_term_));
    }
    res->set_term(current_term_);
    res->set_vote_granted(can_vote);
  }

  void HandleAppendEntries(const raft::AppendEntriesRequest*  req,
                                 raft::AppendEntriesResponse* res) {
    std::lock_guard<std::mutex> g(mu_);

    if (req->term() < current_term_) {
      res->set_term(current_term_);
      res->set_success(false);
      return;
    }

    if (req->term() > current_term_ || role_ == Role::CANDIDATE)
      BecomeFollower(req->term());

    // Valid leader contact: reset election timer
    last_heartbeat_received_ = std::chrono::steady_clock::now();
    leader_id_               = req->leader_id();

    int64_t prev_idx  = req->prev_log_index();
    int64_t prev_term = req->prev_log_term();

    // Missing entries
    if (prev_idx >= (int64_t)log_.size()) {
      res->set_term(current_term_);
      res->set_success(false);
      res->set_conflict_index(static_cast<int64_t>(log_.size()));
      res->set_conflict_term(0);
      return;
    }

    // Term mismatch at prevLogIndex
    if (prev_idx > 0 && log_[prev_idx].term != prev_term) {
      int64_t ct    = log_[prev_idx].term;
      int64_t first = prev_idx;
      while (first > 1 && log_[first - 1].term == ct) --first;
      res->set_term(current_term_);
      res->set_success(false);
      res->set_conflict_index(first);
      res->set_conflict_term(ct);
      return;
    }

    // Append / overwrite entries
    bool truncated = false;
    for (int i = 0; i < req->entries_size(); i++) {
      int64_t log_idx = prev_idx + 1 + i;
      const auto& e   = req->entries(i);

      if (log_idx < (int64_t)log_.size()) {
        if (log_[log_idx].term != e.term()) {
          log_.resize(log_idx);
          truncated = true;
        }
      }

      if (log_idx >= (int64_t)log_.size()) {
        RaftEntry entry{e.term(), e.command()};
        log_.push_back(entry);
      }
    }

    if (truncated || req->entries_size() > 0) SaveFullLog();

    if (req->leader_commit() > commit_index_) {
      commit_index_ =
          std::min(req->leader_commit(), (int64_t)log_.size() - 1);
      SaveMeta();
      apply_cv_.notify_all();
    }

    res->set_term(current_term_);
    res->set_success(true);
  }

 private:
  enum class Role { FOLLOWER, CANDIDATE, LEADER };

  // ── Identity ────────────────────────────────────────────────────
  int my_id_;
  int rf_;
  std::vector<std::string> all_api_addrs_;
  std::vector<std::string> all_p2p_addrs_;
  std::string              backer_path_;
  std::function<KVResult(const KVCmd&)> apply_fn_;

  // ── Persistent state ─────────────────────────────────────────────
  mutable std::mutex mu_;
  int64_t            current_term_ = 0;
  int                voted_for_    = -1;
  std::vector<RaftEntry> log_;  // 1-indexed; log_[0] is sentinel

  // ── Volatile state ───────────────────────────────────────────────
  int64_t commit_index_            = 0;
  int64_t last_applied_            = 0;
  Role    role_                    = Role::FOLLOWER;
  int     leader_id_               = -1;

  // ── Leader volatile state ────────────────────────────────────────
  std::vector<int64_t> next_index_;
  std::vector<int64_t> match_index_;

  // ── Timing ───────────────────────────────────────────────────────
  std::chrono::steady_clock::time_point last_heartbeat_received_;
  std::mt19937 rng_;

  // ── Thread signals ────────────────────────────────────────────────
  std::condition_variable repl_cv_;   // wakes ReplicationLoop
  std::condition_variable apply_cv_;  // wakes ApplyLoop
  std::condition_variable elect_cv_;  // wakes ElectionLoop on Stop

  // ── Pending submissions ──────────────────────────────────────────
  std::map<int64_t, std::promise<KVResult>> pending_;

  // ── Background threads ───────────────────────────────────────────
  std::thread election_thread_;
  std::thread replication_thread_;
  std::thread apply_thread_;
  std::atomic<bool> stopped_{true};

  // ── gRPC stubs ────────────────────────────────────────────────────
  std::vector<std::unique_ptr<raft::RaftService::Stub>> peer_stubs_;

  // ─────────────────────────────────────────────────────────────────
  // Helpers (all called under mu_ unless noted)
  // ─────────────────────────────────────────────────────────────────

  int64_t LastLogIndex() const { return static_cast<int64_t>(log_.size()) - 1; }
  int64_t LastLogTerm()  const { return log_.back().term; }
  int64_t LogTerm(int64_t idx) const {
    if (idx <= 0 || idx >= (int64_t)log_.size()) return 0;
    return log_[idx].term;
  }

  std::string LeaderApiAddrLocked() const {
    if (leader_id_ >= 0 && leader_id_ < rf_)
      return all_api_addrs_[leader_id_];
    return "";
  }

  int GetElectionTimeoutMs() {
    std::uniform_int_distribution<int> dist(2000, 4000);
    return dist(rng_);
  }

  void RaftLog(const std::string& msg) const {
    const auto now = std::chrono::system_clock::now();
    const auto t   = std::chrono::system_clock::to_time_t(now);
    std::ostringstream oss;
    oss << std::put_time(std::localtime(&t), "%F %T");
    std::cerr << "[" << oss.str() << "] [raft id=" << my_id_
              << " term=" << current_term_ << "] " << msg << "\n";
  }

  // ── Role transitions (called under mu_) ──────────────────────────

  void BecomeFollower(int64_t new_term) {
    bool was_leader = (role_ == Role::LEADER);
    role_         = Role::FOLLOWER;
    current_term_ = new_term;
    voted_for_    = -1;
    SaveMeta();
    if (was_leader) {
      RaftLog("stepped down from leader");
      CancelPending();
    }
  }

  void BecomeCandidate() {
    ++current_term_;
    role_      = Role::CANDIDATE;
    voted_for_ = my_id_;
    last_heartbeat_received_ = std::chrono::steady_clock::now();
    SaveMeta();
    RaftLog("became candidate");
  }

  void BecomeLeader() {
    role_      = Role::LEADER;
    leader_id_ = my_id_;
    next_index_.assign(rf_,  LastLogIndex() + 1);
    match_index_.assign(rf_, 0);
    match_index_[my_id_] = LastLogIndex();

    // Raft §8: append a no-op entry in the new term so that entries from
    // previous terms get committed as soon as a majority replicates this entry.
    KVCmd noop{KVCmdType::NOOP, "", ""};
    RaftEntry noop_entry{current_term_, noop.Serialize()};
    AppendLogEntryLocked(noop_entry);
    match_index_[my_id_] = LastLogIndex();
    AdvanceCommitIndex();  // For RF=1, the no-op commits immediately

    RaftLog("became LEADER");
    repl_cv_.notify_all();
  }

  // Cancel all pending Submit() futures (called when stepping down)
  void CancelPending() {
    for (auto& [idx, prom] : pending_) {
      try {
        prom.set_exception(
            std::make_exception_ptr(NotLeaderError("")));
      } catch (...) {}
    }
    pending_.clear();
  }

  // ── Commit index advancement (called under mu_) ───────────────────

  void AdvanceCommitIndex() {
    for (int64_t n = (int64_t)log_.size() - 1; n > commit_index_; --n) {
      if (log_[n].term != current_term_) continue;
      int count = 0;
      for (int i = 0; i < rf_; i++)
        if (match_index_[i] >= n) ++count;
      if (count > rf_ / 2) {
        commit_index_ = n;
        SaveMeta();
        apply_cv_.notify_all();
        return;
      }
    }
  }

  // ── Persistence ───────────────────────────────────────────────────

  void SaveMeta() const {
    std::string path = backer_path_ + "/raft_meta";
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return;
    char buf[20];
    std::memcpy(&buf[0],  &current_term_,  8);
    std::memcpy(&buf[8],  &voted_for_,     4);
    std::memcpy(&buf[12], &commit_index_,  8);
    (void)::write(fd, buf, 20);
#ifdef __APPLE__
    ::fsync(fd);
#else
    ::fdatasync(fd);
#endif
    ::close(fd);
  }

  void AppendLogEntryLocked(const RaftEntry& entry) {
    log_.push_back(entry);
    std::string path = backer_path_ + "/raft_log";
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) return;
    uint32_t clen = static_cast<uint32_t>(entry.cmd_bytes.size());
    char hdr[12];
    std::memcpy(&hdr[0], &entry.term, 8);
    std::memcpy(&hdr[8], &clen,       4);
    (void)::write(fd, hdr,                  12);
    (void)::write(fd, entry.cmd_bytes.data(), clen);
#ifdef __APPLE__
    ::fsync(fd);
#else
    ::fdatasync(fd);
#endif
    ::close(fd);
  }

  void SaveFullLog() const {
    std::string path = backer_path_ + "/raft_log";
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return;
    for (size_t i = 1; i < log_.size(); i++) {
      const auto& e   = log_[i];
      uint32_t    clen = static_cast<uint32_t>(e.cmd_bytes.size());
      char hdr[12];
      std::memcpy(&hdr[0], &e.term, 8);
      std::memcpy(&hdr[8], &clen,   4);
      (void)::write(fd, hdr,               12);
      (void)::write(fd, e.cmd_bytes.data(), clen);
    }
#ifdef __APPLE__
    ::fsync(fd);
#else
    ::fdatasync(fd);
#endif
    ::close(fd);
  }

  void LoadState() {
    // Load meta
    {
      std::string path = backer_path_ + "/raft_meta";
      int fd = ::open(path.c_str(), O_RDONLY);
      if (fd >= 0) {
        char buf[20];
        if (::read(fd, buf, 20) == 20) {
          std::memcpy(&current_term_,  &buf[0],  8);
          std::memcpy(&voted_for_,     &buf[8],  4);
          std::memcpy(&commit_index_,  &buf[12], 8);
        }
        ::close(fd);
      }
    }

    // Load log
    {
      std::string path = backer_path_ + "/raft_log";
      int fd = ::open(path.c_str(), O_RDONLY);
      if (fd >= 0) {
        off_t sz = ::lseek(fd, 0, SEEK_END);
        ::lseek(fd, 0, SEEK_SET);
        if (sz > 0) {
          std::vector<char> data(static_cast<size_t>(sz));
          ::read(fd, data.data(), sz);
          size_t pos = 0;
          while (pos + 12 <= static_cast<size_t>(sz)) {
            int64_t  term = 0;
            uint32_t clen = 0;
            std::memcpy(&term, &data[pos],     8);
            std::memcpy(&clen, &data[pos + 8], 4);
            pos += 12;
            if (pos + clen > static_cast<size_t>(sz)) break;
            RaftEntry e{term, std::string(&data[pos], clen)};
            log_.push_back(e);
            pos += clen;
          }
        }
        ::close(fd);
      }
    }
  }

  // ── Background loops ──────────────────────────────────────────────

  void ElectionLoop() {
    while (!stopped_) {
      int timeout_ms = GetElectionTimeoutMs();

      {
        std::unique_lock<std::mutex> g(mu_);
        elect_cv_.wait_for(g,
            std::chrono::milliseconds(timeout_ms),
            [this] { return stopped_.load(); });
      }
      if (stopped_) break;

      bool should_elect = false;
      {
        std::lock_guard<std::mutex> g(mu_);
        if (role_ != Role::LEADER) {
          auto elapsed_ms =
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now() - last_heartbeat_received_)
                  .count();
          if (elapsed_ms >= timeout_ms) {
            BecomeCandidate();
            should_elect = true;
          }
        }
      }
      if (should_elect) RunElection();
    }
  }

  void RunElection() {
    int64_t term, lli, llt;
    {
      std::lock_guard<std::mutex> g(mu_);
      if (role_ != Role::CANDIDATE) return;
      term = current_term_;
      lli  = LastLogIndex();
      llt  = LastLogTerm();
    }

    std::mutex           vote_mu;
    int                  votes = 1;  // vote for self
    std::atomic<bool>    stepped_down{false};

    std::vector<std::thread> threads;
    for (int i = 0; i < rf_; i++) {
      if (i == my_id_ || !peer_stubs_[i]) continue;
      threads.emplace_back([&, i]() {
        raft::RequestVoteRequest req;
        req.set_term(term);
        req.set_candidate_id(my_id_);
        req.set_last_log_index(lli);
        req.set_last_log_term(llt);

        raft::RequestVoteResponse res;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(1500));
        auto st = peer_stubs_[i]->RequestVote(&ctx, req, &res);
        if (!st.ok()) return;

        std::lock_guard<std::mutex> g(mu_);
        if (res.term() > current_term_) {
          BecomeFollower(res.term());
          stepped_down = true;
          return;
        }
        if (role_ != Role::CANDIDATE || current_term_ != term) return;
        if (res.vote_granted()) {
          std::lock_guard<std::mutex> vg(vote_mu);
          ++votes;
        }
      });
    }
    for (auto& t : threads) if (t.joinable()) t.join();

    if (stepped_down) return;

    std::lock_guard<std::mutex> g(mu_);
    if (role_ != Role::CANDIDATE || current_term_ != term) return;
    int v;
    { std::lock_guard<std::mutex> vg(vote_mu); v = votes; }
    if (v > rf_ / 2) BecomeLeader();
  }

  void ReplicationLoop() {
    while (!stopped_) {
      {
        std::unique_lock<std::mutex> g(mu_);
        repl_cv_.wait_for(g,
            std::chrono::milliseconds(500),
            [this] { return stopped_.load(); });
      }
      if (stopped_) break;

      int64_t term;
      bool    is_leader;
      {
        std::lock_guard<std::mutex> g(mu_);
        is_leader = (role_ == Role::LEADER);
        term      = current_term_;
      }
      if (!is_leader) continue;

      ReplicateToAllPeers(term);
    }
  }

  void ReplicateToAllPeers(int64_t term_snap) {
    std::vector<std::thread> threads;
    for (int i = 0; i < rf_; i++) {
      if (i == my_id_) continue;
      threads.emplace_back([this, i, term_snap]() {
        ReplicateToPeer(i, term_snap);
      });
    }
    for (auto& t : threads) if (t.joinable()) t.join();
  }

  void ReplicateToPeer(int peer_id, int64_t term_snap) {
    if (!peer_stubs_[peer_id]) return;

    raft::AppendEntriesRequest req;

    {
      std::lock_guard<std::mutex> g(mu_);
      if (role_ != Role::LEADER || current_term_ != term_snap) return;

      int64_t ni       = next_index_[peer_id];
      int64_t prev_idx = ni - 1;

      req.set_term(current_term_);
      req.set_leader_id(my_id_);
      req.set_prev_log_index(prev_idx);
      req.set_prev_log_term(LogTerm(prev_idx));
      req.set_leader_commit(commit_index_);

      for (int64_t j = ni; j < (int64_t)log_.size(); j++) {
        auto* e = req.add_entries();
        e->set_term(log_[j].term);
        e->set_command(log_[j].cmd_bytes);
      }
    }

    raft::AppendEntriesResponse res;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::milliseconds(1500));
    auto st = peer_stubs_[peer_id]->AppendEntries(&ctx, req, &res);
    if (!st.ok()) return;

    std::lock_guard<std::mutex> g(mu_);
    if (role_ != Role::LEADER || current_term_ != term_snap) return;

    if (res.term() > current_term_) {
      BecomeFollower(res.term());
      return;
    }

    if (res.success()) {
      int64_t new_match =
          req.prev_log_index() + static_cast<int64_t>(req.entries_size());
      if (new_match > match_index_[peer_id]) {
        match_index_[peer_id] = new_match;
        next_index_[peer_id]  = new_match + 1;
      }
      AdvanceCommitIndex();
    } else {
      // Fast backtracking
      if (res.conflict_index() > 0) {
        // Find last entry in our log with conflict_term
        bool found = false;
        if (res.conflict_term() > 0) {
          for (int64_t j = (int64_t)log_.size() - 1; j >= 1; j--) {
            if (log_[j].term == res.conflict_term()) {
              next_index_[peer_id] = j + 1;
              found = true;
              break;
            }
          }
        }
        if (!found) next_index_[peer_id] = res.conflict_index();
      } else {
        next_index_[peer_id] =
            std::max(int64_t{1}, next_index_[peer_id] - 1);
      }
      if (next_index_[peer_id] < 1) next_index_[peer_id] = 1;
    }
  }

  void ApplyLoop() {
    while (!stopped_) {
      std::unique_lock<std::mutex> g(mu_);
      apply_cv_.wait(g, [this] {
        return stopped_.load() || last_applied_ < commit_index_;
      });
      if (stopped_ && last_applied_ >= commit_index_) break;

      while (last_applied_ < commit_index_) {
        int64_t idx = last_applied_ + 1;
        if (idx >= (int64_t)log_.size()) break;

        KVCmd cmd = KVCmd::Deserialize(log_[idx].cmd_bytes);

        std::optional<std::promise<KVResult>> prom;
        auto it = pending_.find(idx);
        if (it != pending_.end()) {
          prom = std::move(it->second);
          pending_.erase(it);
        }
        last_applied_ = idx;

        g.unlock();
        KVResult result = apply_fn_(cmd);
        if (prom.has_value()) {
          try { prom->set_value(result); } catch (...) {}
        }
        g.lock();
      }
    }

    // Cancel any remaining pending on shutdown
    std::lock_guard<std::mutex> g(mu_);
    CancelPending();
  }
};
