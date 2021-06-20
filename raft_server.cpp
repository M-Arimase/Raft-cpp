#include "./raft_server.h"

namespace raft {

template <int S> raft_server<S>::raft_server(char *config_path) {
  current_state = RAFT_STATE_FOLLOWER;

  current_term = 0;
  voted_for = 0;
  nb_rv_accept = 0;

  last_log_index = 0;
  last_log_term = 0;

  commit_index = 0;
  last_applied = 0;

  memset(next_index, 0, sizeof(next_index));
  memset(match_index, 0, sizeof(match_index));

  ifstream config_loader(config_path);
  for (int i = 0; i < S; i++) {
    config_loader >> peer_address[i];
    peer_convert[peer_address[i]] = i;

    const string endpoint =
        string("tcp://localhost:") + to_string(peer_address[i]);

    peer_socket[i] = new zmqpp::socket(peer_context, zmqpp::socket_type::push);
    peer_socket[i]->connect(endpoint);
  }
}

template <int S> uint32_t raft_server<S>::current_time_point() {
  return chrono::duration_cast<chrono::microseconds>(
             chrono::system_clock::now().time_since_epoch())
      .count();
}

template <int S>
uint8_t raft_server<S>::classify_message(zmqpp::message &message) {
  string message_type;
  message >> message_type;

  if (message_type == "TimeoutAppendEntries") {
    return RAFT_TIMEOUT_AE;
  }
  if (message_type == "TimeoutHeartBeat") {
    return RAFT_TIMEOUT_HB;
  }
  if (message_type == "RequestVote") {
    return RAFT_RPC_RV;
  }
  if (message_type == "AppendEntries") {
    return RAFT_RPC_AE;
  }
  if (message_type == "RequestVoteReply") {
    return RAFT_REPLY_RV;
  }

  return 0;
}

template <int S>
void raft_server<S>::send_message(uint32_t peer_address,
                                  zmqpp::message &message, string reason) {
  cout << "send message: " << peer_address << " " << reason << endl;
  peer_socket[peer_convert[peer_address]]->send(message);
}

template <int S> uint32_t raft_server<S>::timeout_ae() {
  static uniform_int_distribution<unsigned> u(timeout_ae_min, timeout_ae_max);
  static default_random_engine e(time(nullptr));
  return u(e);
}

template <int S> void raft_server<S>::raft_timeout_ae_helper() {
  timeout_ts_ae = current_time_point() + timeout_ae();

  while (true) {
    while (current_time_point() < timeout_ts_ae) {
      uint32_t timeout_elapses =
          min(timeout_ae_min, timeout_ts_ae - current_time_point());
      this_thread::sleep_for(chrono::microseconds(timeout_elapses));
    }

    zmqpp::message message;
    message << "TimeoutAppendEntries";
    send_message(peer_address[0], message, "TimeoutAppendEntries");

    timeout_ts_ae = current_time_point() + timeout_ae();
  }
}

template <int S> void raft_server<S>::handle_timeout_ae() {
  cout << "Handle Timeout AppendEntries" << endl;

  current_state = RAFT_STATE_CANDIDATE;

  current_term += 1;
  voted_for = peer_address[0];

  raft_message_rv rpc_rv;
  rpc_rv.term = current_term;
  rpc_rv.candidate_id = peer_address[0];
  rpc_rv.last_log_index = last_log_index;
  rpc_rv.last_log_term = last_log_term;

  nb_rv_accept = 0;
  for (int i = 0; i < S; i++) {
    send_rpc_rv(peer_address[i], rpc_rv);
  }
}

template <int S> void raft_server<S>::raft_timeout_hb_helper() {
  timeout_ts_hb = current_time_point() + timeout_hb;

  while (true) {
    while (current_time_point() < timeout_ts_hb) {
      this_thread::sleep_for(
          chrono::microseconds(timeout_ts_hb - current_time_point()));
    }

    if (current_state == RAFT_STATE_LEADER) {
      zmqpp::message message;
      message << "TimeoutHeartBeat";
      send_message(peer_address[0], message, "TimeoutHeartBeat");
    }
    timeout_ts_hb = current_time_point() + timeout_hb;
  }
}

template <int S> void raft_server<S>::handle_timeout_hb() {
  cout << "Handle Timeout HeartBeat" << endl;

  raft_message_ae rpc_ae;
  rpc_ae.term = current_term;
  rpc_ae.leader_id = peer_address[0];
  rpc_ae.prev_log_index = last_log_index;
  rpc_ae.prev_log_term = last_log_term;
  rpc_ae.nb_entries = 0;
  rpc_ae.entries.clear();
  rpc_ae.leader_commit = commit_index;

  for (int i = 0; i < S; i++) {
    send_rpc_ae(peer_address[i], rpc_ae);
  }
}

template <int S>
void raft_server<S>::send_rpc_rv(uint32_t peer_address,
                                 raft_message_rv &rpc_rv) {
  zmqpp::message message;
  message << "RequestVote";

  message << rpc_rv.term;
  message << rpc_rv.candidate_id;
  message << rpc_rv.last_log_index;
  message << rpc_rv.last_log_term;

  send_message(peer_address, message, "RequestVote");
}

template <int S> void raft_server<S>::handle_rpc_rv(zmqpp::message &message) {
  cout << "Handle RPC RequestVote" << endl;

  raft_message_rv rpc_rv;
  message >> rpc_rv.term;
  message >> rpc_rv.candidate_id;
  message >> rpc_rv.last_log_index;
  message >> rpc_rv.last_log_term;

  cout << "term: " << rpc_rv.term << endl;
  cout << "candidate_id: " << rpc_rv.candidate_id << endl;
  cout << "last_log_index: " << rpc_rv.last_log_index << endl;
  cout << "last_log_term: " << rpc_rv.last_log_term << endl;

  if (rpc_rv.term > current_term) {
    current_term = rpc_rv.term;
    current_state = RAFT_STATE_FOLLOWER;

    voted_for = 0;
  }

  if ((rpc_rv.term < current_term) ||
      (voted_for > 0 && voted_for != rpc_rv.candidate_id) ||
      (rpc_rv.last_log_term < last_log_term) ||
      (rpc_rv.last_log_term == last_log_term &&
       rpc_rv.last_log_index < last_log_index)) {
    send_reply_rv_reject(rpc_rv);
  } else {
    voted_for = rpc_rv.candidate_id;
    send_reply_rv_accept(rpc_rv);
  }
}

template <int S>
void raft_server<S>::send_reply_rv_accept(raft_message_rv &rpc_rv) {
  zmqpp::message message;
  message << "RequestVoteReply" << current_term << true;
  send_message(rpc_rv.candidate_id, message, "RequestVoteReply:True");
}

template <int S>
void raft_server<S>::send_reply_rv_reject(raft_message_rv &rpc_rv) {
  zmqpp::message message;
  message << "RequestVoteReply" << current_term << false;
  send_message(rpc_rv.candidate_id, message, "RequestVoteReply:False");
}

template <int S> void raft_server<S>::handle_reply_rv(zmqpp::message &message) {
  uint32_t reply_term;
  bool voted;

  message >> reply_term;
  message >> voted;

  if (reply_term > current_term) {
    current_term = reply_term;
    current_state = RAFT_STATE_FOLLOWER;

    voted_for = 0;
  }

  if (current_state == RAFT_STATE_CANDIDATE && reply_term == current_term &&
      voted == true) {
    nb_rv_accept += 1;
    if (nb_rv_accept * 2 > S) {
      current_state = RAFT_STATE_LEADER;

      zmqpp::message message;
      message << "TimeoutHeartBeat";
      send_message(peer_address[0], message, "TimeoutHeartBeat");
      timeout_ts_hb = current_time_point() + timeout_hb;
    }
  }
}

template <int S>
void raft_server<S>::send_rpc_ae(uint32_t peer_address,
                                 raft_message_ae &rpc_ae) {
  zmqpp::message message;
  message << "AppendEntries";

  message << rpc_ae.term;
  message << rpc_ae.leader_id;
  message << rpc_ae.prev_log_index;
  message << rpc_ae.prev_log_term;
  message << rpc_ae.nb_entries;
  for (raft_log &entry : rpc_ae.entries) {
    message << entry.term << entry.command;
  }
  message << rpc_ae.leader_commit;

  send_message(peer_address, message, "AppendEntries");
}

template <int S> void raft_server<S>::handle_rpc_ae(zmqpp::message &message) {
  cout << "Handle RPC AppendEntries" << endl;

  raft_message_ae rpc_ae;
  message >> rpc_ae.term;
  message >> rpc_ae.leader_id;
  message >> rpc_ae.prev_log_index;
  message >> rpc_ae.prev_log_term;
  message >> rpc_ae.nb_entries;
  rpc_ae.entries.clear();
  for (uint32_t i = 0; i < rpc_ae.nb_entries; i++) {
    raft_log entry;
    message >> entry.term >> entry.command;
    rpc_ae.entries.push_back(entry);
  }
  message >> rpc_ae.leader_commit;

  cout << "term: " << rpc_ae.term << endl;
  cout << "leader_id: " << rpc_ae.leader_id << endl;
  cout << "prev_log_index: " << rpc_ae.prev_log_index << endl;
  cout << "prev_log_term: " << rpc_ae.prev_log_term << endl;
  for (raft_log &entry : rpc_ae.entries) {
    cout << "entry: " << entry.term << " " << entry.command << endl;
  }
  cout << "leader_commit: " << rpc_ae.leader_commit << endl;

  if (rpc_ae.term >= current_term) {
    current_term = rpc_ae.term;
    if (rpc_ae.leader_id != peer_address[0]) {
      current_state = RAFT_STATE_FOLLOWER;
    }
    voted_for = rpc_ae.leader_id;

    cout << "Acknowledge Leader: " << voted_for << endl;
    timeout_ts_ae = current_time_point() + timeout_ae();
  }
}

template <int S> void raft_server<S>::main_loop() {
  const string endpoint = string("tcp://*:") + to_string(peer_address[0]);

  zmqpp::context recv_context;

  zmqpp::socket_type recv_type = zmqpp::socket_type::pull;
  zmqpp::socket recv_socket(recv_context, recv_type);

  recv_socket.bind(endpoint);

  thread timeout_thread_ae(raft_timeout_ae, this);
  thread timeout_thread_hb(raft_timeout_hb, this);

  while (true) {
    cout << "Main Loop" << endl;
    zmqpp::message message;

    recv_socket.receive(message);

    uint8_t message_type = classify_message(message);

    assert(message_type != 0);

    if (message_type == RAFT_TIMEOUT_AE) {
      handle_timeout_ae();
    }
    if (message_type == RAFT_TIMEOUT_HB) {
      handle_timeout_hb();
    }
    if (message_type == RAFT_RPC_RV) {
      handle_rpc_rv(message);
    }
    if (message_type == RAFT_RPC_AE) {
      handle_rpc_ae(message);
    }
    if (message_type == RAFT_REPLY_RV) {
      handle_reply_rv(message);
    }
  }

  timeout_thread_ae.join();
  timeout_thread_hb.join();
}

}; // namespace raft

int main(int argc, char **argv) {
  assert(argc == 2);

  raft::raft_server<3> server(argv[1]);

  server.main_loop();
}
