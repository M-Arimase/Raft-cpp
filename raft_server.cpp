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
  if (message_type == "AppendEntriesReply") {
    return RAFT_REPLY_AE;
  }
  if (message_type == "ClientCommand") {
    return RAFT_CLIENT_CMD;
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
  send_rpc_ae_helper(rpc_ae, last_log_index + 1);

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
  cout << "Handle Replay RequestVote ";

  uint32_t reply_term;
  bool vote_granted;

  message >> reply_term;
  message >> vote_granted;

  cout << vote_granted << endl;

  if (reply_term > current_term) {
    current_term = reply_term;
    current_state = RAFT_STATE_FOLLOWER;

    voted_for = 0;
  }

  if (current_state == RAFT_STATE_CANDIDATE && reply_term == current_term &&
      vote_granted == true) {
    nb_rv_accept += 1;
    if (nb_rv_accept * 2 > S) {
      current_state = RAFT_STATE_LEADER;
      for (int i = 0; i < S; i++) {
        next_index[i] = last_log_index + 1;
        match_index[i] = 0;
      }

      zmqpp::message message;
      message << "TimeoutHeartBeat";
      send_message(peer_address[0], message, "TimeoutHeartBeat");
      timeout_ts_hb = current_time_point() + timeout_hb;
    }
  }
}

template <int S>
void raft_server<S>::send_rpc_ae_helper(raft_message_ae &rpc_ae,
                                        uint32_t next_index) {
  rpc_ae.term = current_term;
  rpc_ae.leader_id = peer_address[0];

  rpc_ae.prev_log_index = next_index - 1;
  if (rpc_ae.prev_log_index > 0) {
    rpc_ae.prev_log_term = log[rpc_ae.prev_log_index - 1].term;
  } else {
    rpc_ae.prev_log_term = 0;
  }

  rpc_ae.nb_entries = last_log_index - next_index + 1;
  rpc_ae.entries.clear();
  for (uint32_t i = next_index; i <= last_log_index; i++) {
    rpc_ae.entries.push_back(log[i - 1]);
  }

  rpc_ae.leader_commit = commit_index;
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

  if ((rpc_ae.term < current_term) ||
      (rpc_ae.prev_log_index > last_log_index ||
       (rpc_ae.prev_log_index > 0 &&
        rpc_ae.prev_log_term != log[rpc_ae.prev_log_index - 1].term))) {
    send_reply_ae_reject(rpc_ae);
  } else {
    for (uint32_t i = 1; i <= rpc_ae.nb_entries; i++) {
      uint32_t log_index = rpc_ae.prev_log_index + i;

      if (log_index <= last_log_index &&
          rpc_ae.entries[i - 1].term != log[log_index - 1].term) {
        while (log_index <= last_log_index) {
          log.pop_back();
          last_log_index -= 1;
        }

        for (; i < rpc_ae.nb_entries; i++) {
          log.push_back(rpc_ae.entries[i - 1]);
          last_log_index += 1;
        }
      } else if (log_index == last_log_index + 1) {
        log.push_back(rpc_ae.entries[i - 1]);
        last_log_index += 1;
      }
    }

    if (rpc_ae.leader_commit > commit_index) {
      commit_index = min(rpc_ae.leader_commit, last_log_index);
    }
    send_reply_ae_accept(rpc_ae);
  }
}

template <int S>
void raft_server<S>::send_reply_ae_accept(raft_message_ae &rpc_ae) {
  uint32_t match_index = rpc_ae.prev_log_index + rpc_ae.nb_entries;
  zmqpp::message message;
  message << "AppendEntriesReply" << current_term << true << peer_address[0]
          << match_index;
  send_message(rpc_ae.leader_id, message, "AppendEntriesReply:True");
}

template <int S>
void raft_server<S>::send_reply_ae_reject(raft_message_ae &rpc_ae) {
  zmqpp::message message;
  message << "AppendEntriesReply" << current_term << false << peer_address[0];
  send_message(rpc_ae.leader_id, message, "AppendEntriesReply:False");
}

template <int S> void raft_server<S>::handle_reply_ae(zmqpp::message &message) {
  cout << "Handle Replay AppendEntries ";

  uint32_t reply_term;
  bool success;

  uint32_t peer_index;
  uint32_t peer_log_index;

  message >> reply_term;
  message >> success;

  cout << success << endl;

  message >> peer_index;
  peer_index = peer_convert[peer_index];

  if (reply_term > current_term) {
    current_term = reply_term;
    current_state = RAFT_STATE_FOLLOWER;

    voted_for = 0;
  }

  if (current_state == RAFT_STATE_LEADER && reply_term == current_term) {
    if (success == true) {
      message >> peer_log_index;

      match_index[peer_index] = peer_log_index;
      next_index[peer_index] = match_index[peer_index] + 1;

      for (uint32_t log_index = commit_index + 1; log_index <= peer_log_index;
           log_index++) {
        int nb_ae_accept = 0;
        for (int i = 0; i < S; i++) {
          if (match_index[i] >= log_index) {
            nb_ae_accept += 1;
          }
        }
        if (nb_ae_accept * 2 > S && log[log_index - 1].term == current_term) {
          commit_index = log_index;
        }
      }
    } else {
      next_index[peer_index] = max(1u, next_index[peer_index] - 1);

      raft_message_ae rpc_ae;
      send_rpc_ae_helper(rpc_ae, next_index[peer_index]);

      send_rpc_ae(peer_address[peer_index], rpc_ae);
    }
  }
}

template <int S>
void raft_server<S>::handle_client_cmd(zmqpp::message &message) {
  cout << "Handle Client Command" << endl;

  string command;
  message >> command;

  if (current_state == RAFT_STATE_FOLLOWER && voted_for != 0) {
    zmqpp::message redirect_message;
    redirect_message << "ClientCommand" << command;
    send_message(voted_for, redirect_message, "Rediect Client Command");
  }
  if (current_state == RAFT_STATE_LEADER) {
    raft_log entry;
    entry.term = current_term;
    entry.command = command;

    log.push_back(entry);
    last_log_term = current_term;
    last_log_index += 1;
    for (int i = 0; i < S; i++) {
      raft_message_ae rpc_ae;
      send_rpc_ae_helper(rpc_ae, next_index[i]);
      send_rpc_ae(peer_address[i], rpc_ae);
    }
  }
}

template <int S> void raft_server<S>::handle_commit() {
  while (commit_index > last_applied) {
    last_applied += 1;
    cout << "Apply Log: " << last_applied << " " << log[last_applied - 1].term
         << " " << log[last_applied - 1].command << endl;
    state_machine.push_back(log[last_applied - 1]);
  }

  cout << "Last Applied: " << last_applied << endl;
  cout << "Last Log Index: " << last_log_index << endl;

  for (uint32_t i = 1; i <= last_applied; i++) {
    cout << "committed: " << log[i - 1].command << endl;
  }
  for (uint32_t i = last_applied + 1; i <= last_log_index; i++) {
    cout << "uncommitted: " << log[i - 1].command << endl;
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

    handle_commit();

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
    if (message_type == RAFT_REPLY_AE) {
      handle_reply_ae(message);
    }
    if (message_type == RAFT_CLIENT_CMD) {
      handle_client_cmd(message);
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
