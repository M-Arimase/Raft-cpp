#include <bits/stdc++.h>
#include <zmqpp/zmqpp.hpp>

using namespace std;

void main_loop() {
  while (true) {
    uint32_t address;
    cout << "address: ";
    cin >> address;

    string command;
    cout << "command: ";
    cin >> command;

    const string endpoint = "tcp://localhost:" + to_string(address);

    zmqpp::context context;

    zmqpp::socket_type type = zmqpp::socket_type::push;
    zmqpp::socket socket(context, type);

    socket.connect(endpoint);

    zmqpp::message message;
    message << "ClientCommand" << command;
    socket.send(message);
  }
}

int main() { main_loop(); }
