#pragma once

#include <vector>
#include <mutex>
#include <chrono>
#include <memory>
#include <zmq.hpp>

enum class Liveness : uint8_t {
    Alive = 1,
    Dead = 2,
    Uninitialized = 3
};

struct PeerInfo {
    float load = 0.0f;
    std::chrono::steady_clock::time_point last_heartbeat;
    std::unique_ptr<zmq::socket_t> dealer;//tab de socket DEALER connectée vers chacun des autres serveurs du cluster. 
    Liveness status = Liveness::Uninitialized;
};

// Définies dans server.cpp, accessibles depuis load_balancer.cpp et repl.cpp
extern std::vector<PeerInfo> peers;
extern std::mutex peers_mutex;
extern int my_node_id;
extern int num_nodes;
