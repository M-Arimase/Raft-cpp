#include <bits/stdc++.h>
#include <zmqpp/zmqpp.hpp>

using namespace std;

namespace raft {

#define RAFT_STATE_LEADER (0x01)
#define RAFT_STATE_FOLLOWER (0x02)
#define RAFT_STATE_CANDIDATE (0x04)

#define RAFT_TIMEOUT_AE (0x01)
#define RAFT_TIMEOUT_HB (0x02)
#define RAFT_RPC_RV (0x04)
#define RAFT_RPC_AE (0x08)
#define RAFT_REPLY_RV (0x10)
#define RAFT_REPLY_AE (0x20)

class raft_log {
public:
  uint32_t term;
  string command;
};

class raft_message_rv {
public:
  uint32_t term;
  uint32_t candidate_id;

  uint32_t last_log_index;
  uint32_t last_log_term;
};

class raft_message_ae {
public:
  uint32_t term;
  uint32_t leader_id;

  uint32_t prev_log_index;
  uint32_t prev_log_term;

  uint32_t nb_entries;
  vector<raft_log> entries;

  uint32_t leader_commit;
};

template <int S> class raft_server {
public:
  uint8_t current_state;

  uint32_t current_term;
  uint32_t voted_for;
  uint32_t nb_rv_accept;

  vector<raft_log> log;
  uint32_t last_log_index;
  uint32_t last_log_term;

  uint32_t commit_index;
  uint32_t last_applied;

  uint32_t next_index[S];
  uint32_t match_index[S];

  uint32_t peer_address[S];
  unordered_map<uint32_t, uint32_t> peer_convert;

  zmqpp::context peer_context;
  zmqpp::socket *peer_socket[S];

  uint32_t timeout_ts_ae;
  uint32_t timeout_ts_hb;

  static const uint32_t timeout_ae_min = 1500 * 1000;
  static const uint32_t timeout_ae_max = 3000 * 1000;
  static const uint32_t timeout_hb = 100 * 1000;

  raft_server(char *config_path);

  uint32_t current_time_point();

  uint8_t classify_message(zmqpp::message &message);

  void send_message(uint32_t peer_address, zmqpp::message &message,
                    string reason);

  uint32_t timeout_ae();

  static void raft_timeout_ae(raft_server *server) {
    server->raft_timeout_ae_helper();
  }

  void raft_timeout_ae_helper();

  void handle_timeout_ae();

  static void raft_timeout_hb(raft_server *server) {
    server->raft_timeout_hb_helper();
  }

  void raft_timeout_hb_helper();

  void handle_timeout_hb();

  void send_rpc_rv(uint32_t peer_address, raft_message_rv &rpc_rv);

  void handle_rpc_rv(zmqpp::message &message);

  void send_reply_rv_accept(raft_message_rv &rpc_rv);

  void send_reply_rv_reject(raft_message_rv &rpc_rv);

  void handle_reply_rv(zmqpp::message &message);

  void send_rpc_ae(uint32_t peer_address, raft_message_ae &rpc_ae);

  void handle_rpc_ae(zmqpp::message &message);

  void main_loop();
};
}; // namespace raft
