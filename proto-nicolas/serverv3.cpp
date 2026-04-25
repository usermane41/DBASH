#include "message_types.hpp"
#include "cluster_state.hpp"

#include <zmq.hpp>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <sstream>
#include <chrono>
#include <memory>
#include <thread>
#include <mutex>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <load_balancer.hpp>
#include <random>
#include <map>

// ---- Définition des variables globales (déclarées extern dans cluster_state.hpp) ----
std::vector<PeerInfo> peers;
std::mutex peers_mutex;
int my_node_id;
int num_nodes;

// ---- Map des redirections en attente : source_node_id → client_sender_id ----
std::map<int, std::string> pending_redirections;

std::string getLoadAverage() {
    std::ifstream load_file("/proc/loadavg");
    if (!load_file.is_open()) return "error reading loadavg";

    std::string line;
    std::getline(load_file, line);
    
    std::istringstream iss(line);
    std::string firstNumber;
    iss >> firstNumber;

    return firstNumber;
}

void heartbeat_thread(
    const std::vector<std::string>& addresses,
    zmq::context_t& context
) {
    std::vector<zmq::socket_t> dealers(num_nodes);

    for (int j = 0; j < num_nodes; ++j) {
        if (j == my_node_id) continue;
    
        dealers[j] = zmq::socket_t(context, zmq::socket_type::dealer);
        std::string identity = "heartbeat_" + std::to_string(my_node_id);
        dealers[j].set(zmq::sockopt::routing_id, identity);
        dealers[j].set(zmq::sockopt::linger, 0);
        dealers[j].connect(addresses[j]);
        std::cout << "[HB] Connected to peer " << j << std::endl;
    }

    while (true) {
        auto now = std::chrono::steady_clock::now();
        std::string payload = getLoadAverage();
    
        for (int j = 0; j < num_nodes; ++j) {
            if (j == my_node_id) continue;

            peers_mutex.lock();
            Liveness peer_status = peers[j].status;
            auto last_heartbeat = peers[j].last_heartbeat;
            peers_mutex.unlock();
        
            if (peer_status == Liveness::Alive) {
                auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_heartbeat);
                if (elapsed.count() > 10) {
                    peers_mutex.lock();
                    peers[j].status = Liveness::Dead;
                    peers_mutex.unlock();
                    std::cout << "[HB] Peer " << j << " marked DEAD" << std::endl;
                    continue;
                }
            }

            if (peer_status == Liveness::Alive || peer_status == Liveness::Uninitialized) {
                MessageType type = MessageType::Heartbeat;
                uint8_t type_byte = static_cast<uint8_t>(type);
                zmq::message_t type_frame(sizeof(uint8_t));
                memcpy(type_frame.data(), &type_byte, sizeof(uint8_t));
                zmq::message_t payload_frame(payload.begin(), payload.end());
                dealers[j].send(type_frame, zmq::send_flags::sndmore);
                dealers[j].send(payload_frame, zmq::send_flags::none);
                std::cout << "[HB] Sent to peer " << j << std::endl;
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(3));
    }
}

// ---- Broadcast de charge immédiat vers tous les pairs alive ----
static void broadcast_load() {
    std::string new_load = getLoadAverage();
    for (int j = 0; j < num_nodes; ++j) {
        if (j == my_node_id) continue;
        peers_mutex.lock();
        Liveness st = peers[j].status;
        peers_mutex.unlock();
        if (st != Liveness::Alive) continue;

        MessageType hb_type = MessageType::Heartbeat;
        uint8_t hb_byte = static_cast<uint8_t>(hb_type);
        zmq::message_t hb_type_frame(sizeof(uint8_t));
        memcpy(hb_type_frame.data(), &hb_byte, sizeof(uint8_t));
        zmq::message_t hb_payload(new_load.begin(), new_load.end());
        peers[j].dealer->send(hb_type_frame, zmq::send_flags::sndmore);
        peers[j].dealer->send(hb_payload, zmq::send_flags::none);
    }
}

// ---- Parse une string "cmd arg1 arg2" en vecteur de strings ----
static std::vector<std::string> split_command(const std::string& cmd_str) {
    std::vector<std::string> tokens;
    std::istringstream iss(cmd_str);
    std::string token;
    while (iss >> token) {
        if (token == "&") break;
        tokens.push_back(token);
    }
    return tokens;
}

// ---- Fork/exec + heartbeat réactif, retourne le global_pid (0 si erreur) ----
static uint32_t fork_and_exec(const std::string& message, bool background) {
    std::string cmd_str = background ? message.substr(0, message.size() - 2) : message;
    auto tokens = split_command(cmd_str);
    if (tokens.empty()) return 0;

    pid_t pid = fork();
    if (pid < 0) return 0;

    if (pid == 0) {
        std::vector<char*> exec_argv;
        for (auto& t : tokens) exec_argv.push_back(t.data());
        exec_argv.push_back(nullptr);
        execvp(exec_argv[0], exec_argv.data());
        _exit(127);
    }

    uint32_t global_pid = (static_cast<uint32_t>(my_node_id) << 16) | static_cast<uint32_t>(pid);

    broadcast_load();

    if (!background) {
        int status;
        waitpid(pid, &status, 0);
    }

    return global_pid;
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <node_id>" << std::endl;
        return 1;
    }

    my_node_id = std::stoi(argv[1]);

    const std::string config_path = "./config_servers.txt";
    std::ifstream file(config_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open config file\n";
        return 1;
    }

    std::vector<std::string> addresses;
    std::string line;
    while (std::getline(file, line)) {
        if (!line.empty())
            addresses.push_back(line);
    }

    num_nodes = addresses.size();

    if (my_node_id < 0 || my_node_id >= num_nodes) {
        std::cerr << "Invalid node_id\n";
        return 1;
    }

    std::cout << "Total nodes: " << num_nodes << std::endl;

    zmq::context_t context(1);
    zmq::socket_t router(context, zmq::socket_type::router);

    std::string identity = std::to_string(my_node_id);
    router.set(zmq::sockopt::routing_id, identity);
    router.set(zmq::sockopt::linger, 0);
    router.bind(addresses[my_node_id]);

    std::cout << "Node " << my_node_id << " bound to " << addresses[my_node_id] << std::endl;

    peers.resize(num_nodes);

    for (int j = 0; j < num_nodes; ++j) {
        if (j == my_node_id) continue;

        peers[j].dealer = std::make_unique<zmq::socket_t>(context, zmq::socket_type::dealer);
        peers[j].dealer->set(zmq::sockopt::routing_id, identity);
        peers[j].dealer->set(zmq::sockopt::linger, 0);
        peers[j].dealer->connect(addresses[j]);

        MessageType type = MessageType::Heartbeat;
        std::string payload = getLoadAverage();
        uint8_t type_byte = static_cast<uint8_t>(type);
        zmq::message_t type_frame(sizeof(uint8_t));
        memcpy(type_frame.data(), &type_byte, sizeof(uint8_t));
        zmq::message_t payload_frame(payload.begin(), payload.end());
        peers[j].dealer->send(type_frame, zmq::send_flags::sndmore);
        peers[j].dealer->send(payload_frame, zmq::send_flags::none);

        std::cout << "Connected to node " << j << " and sent load: " << payload << std::endl;
    }

    std::thread hb_thread(heartbeat_thread, std::cref(addresses), std::ref(context));

    // ---- Receive loop ----
    while (true) {
        zmq::message_t sender;
        zmq::message_t type_frame;
        zmq::message_t payload_frame;

        router.recv(sender);
        router.recv(type_frame);
        router.recv(payload_frame);

        std::string sender_id(static_cast<char*>(sender.data()), sender.size());
        MessageType type = static_cast<MessageType>(*static_cast<uint8_t*>(type_frame.data()));
        std::string message(static_cast<char*>(payload_frame.data()), payload_frame.size());

        std::cout << "Message from " << sender_id << " [" << static_cast<int>(type) << "]: " << message << std::endl;

        switch(type) {

            case MessageType::Heartbeat: {
                int indice = (sender_id.substr(0, 10) == "heartbeat_")
                    ? std::stoi(sender_id.substr(10))
                    : std::stoi(sender_id);

                peers_mutex.lock();
                Liveness peer_status = peers[indice].status;
                peers_mutex.unlock();

                if (peer_status == Liveness::Dead) {
                    std::cout << "Peer " << sender_id << " came back to life but we'll pretend we haven't seen anything." << std::endl;
                    continue;
                }

                peers_mutex.lock();
                peers[indice].load = std::stof(message);
                peers[indice].status = Liveness::Alive;
                peers[indice].last_heartbeat = std::chrono::steady_clock::now();
                peers_mutex.unlock();

                break;
            }

            case MessageType::ClientCommand: {
                float my_load = std::stof(getLoadAverage());

                peers_mutex.lock();
                float global_charge = get_global_load(peers);
                int target = select_target(my_node_id, peers);
                peers_mutex.unlock();

                if (my_load > global_charge * 1.5 && target != my_node_id) {
                    // ---- Jitter ----
                    std::random_device rd;
                    std::mt19937 gen(rd());
                    std::uniform_int_distribution<> delay(0, 300);
                    std::this_thread::sleep_for(std::chrono::milliseconds(delay(gen)));

                    // ---- Recalculer après le délai ----
                    peers_mutex.lock();
                    global_charge = get_global_load(peers);
                    target = select_target(my_node_id, peers);
                    peers_mutex.unlock();

                    if (target != my_node_id) {
                        // ---- Redirection ----
                        std::cout << "[LB] Redirecting to node " << target << "\n";
                        pending_redirections[target] = sender_id;

                        MessageType remote_type = MessageType::RemoteCommand;
                        uint8_t remote_byte = static_cast<uint8_t>(remote_type);
                        zmq::message_t remote_type_frame(sizeof(uint8_t));
                        memcpy(remote_type_frame.data(), &remote_byte, sizeof(uint8_t));
                        zmq::message_t remote_payload(message.begin(), message.end());

                        peers[target].dealer->send(remote_type_frame, zmq::send_flags::sndmore);
                        peers[target].dealer->send(remote_payload, zmq::send_flags::none);

                        break; // réponse au client viendra via RemoteReturnValue
                    }
                }

                // ---- Fork/exec local ----
                {
                    bool background = !message.empty() && message.back() == '&';
                    uint32_t global_pid = fork_and_exec(message, background);

                    std::string response_payload = global_pid
                        ? "global_pid:" + std::to_string(global_pid)
                        : "error: fork failed";

                    MessageType response_type = MessageType::ClientReturnValue;
                    uint8_t resp_type_byte = static_cast<uint8_t>(response_type);
                    zmq::message_t resp_type_frame(sizeof(uint8_t));
                    memcpy(resp_type_frame.data(), &resp_type_byte, sizeof(uint8_t));
                    zmq::message_t resp_payload_frame(response_payload.begin(), response_payload.end());

                    router.send(sender, zmq::send_flags::sndmore);
                    router.send(resp_type_frame, zmq::send_flags::sndmore);
                    router.send(resp_payload_frame, zmq::send_flags::none);
                }

                break;
            }

            case MessageType::RemoteCommand: {
                // Un autre nœud nous délègue une commande — fork/exec et répondre
                bool background = !message.empty() && message.back() == '&';
                uint32_t global_pid = fork_and_exec(message, background);

                std::string response_payload = global_pid
                    ? "global_pid:" + std::to_string(global_pid)
                    : "error: fork failed";

                MessageType response_type = MessageType::RemoteReturnValue;
                uint8_t resp_type_byte = static_cast<uint8_t>(response_type);
                zmq::message_t resp_type_frame(sizeof(uint8_t));
                memcpy(resp_type_frame.data(), &resp_type_byte, sizeof(uint8_t));
                zmq::message_t resp_payload_frame(response_payload.begin(), response_payload.end());

                router.send(sender, zmq::send_flags::sndmore);
                router.send(resp_type_frame, zmq::send_flags::sndmore);
                router.send(resp_payload_frame, zmq::send_flags::none);

                break;
            }

            case MessageType::RemoteReturnValue: {
                // Réponse d'un nœud distant — retrouver le client qui attend
                int source_node = std::stoi(sender_id);

                auto it = pending_redirections.find(source_node);
                if (it == pending_redirections.end()) {
                    std::cout << "[ERR] RemoteReturnValue from " << sender_id << " but no pending client\n";
                    break;
                }

                std::string client_id = it->second;
                pending_redirections.erase(it);

                // Transmettre la réponse au client original
                MessageType response_type = MessageType::ClientReturnValue;
                uint8_t resp_type_byte = static_cast<uint8_t>(response_type);
                zmq::message_t resp_type_frame(sizeof(uint8_t));
                memcpy(resp_type_frame.data(), &resp_type_byte, sizeof(uint8_t));
                zmq::message_t resp_payload_frame(message.begin(), message.end());
                zmq::message_t client_frame(client_id.begin(), client_id.end());

                router.send(client_frame, zmq::send_flags::sndmore);
                router.send(resp_type_frame, zmq::send_flags::sndmore);
                router.send(resp_payload_frame, zmq::send_flags::none);

                break;
            }

            case MessageType::ClientHandshake: {
                MessageType response_type = MessageType::ClientAcknowledgement;
                std::string response_payload = "";

                uint8_t resp_type_byte = static_cast<uint8_t>(response_type);
                zmq::message_t resp_type_frame(sizeof(uint8_t));
                memcpy(resp_type_frame.data(), &resp_type_byte, sizeof(uint8_t));
                zmq::message_t resp_payload_frame(response_payload.begin(), response_payload.end());

                router.send(sender, zmq::send_flags::sndmore);
                router.send(resp_type_frame, zmq::send_flags::sndmore);
                router.send(resp_payload_frame, zmq::send_flags::none);

                break;
            }
        
            case MessageType::RemoteKill: {
                auto p1 = message.find(':');
                auto p2 = message.find(':', p1 + 1);
                int target_node = std::stoi(message.substr(0, p1));
                int sig         = std::stoi(message.substr(p1 + 1, p2 - p1 - 1));
                pid_t lpid      = std::stoi(message.substr(p2 + 1));

                if (target_node == my_node_id) {
                    kill(lpid, sig);
                } else {
                    // Forward au nœud cible
                    MessageType remote_type = MessageType::RemoteKill;
                    std::string fwd_payload = std::to_string(target_node) + ":" 
                                            + std::to_string(sig) + ":" 
                                            + std::to_string(lpid);
                    uint8_t type_byte = static_cast<uint8_t>(remote_type);
                    zmq::message_t type_frame(sizeof(uint8_t));
                    memcpy(type_frame.data(), &type_byte, sizeof(uint8_t));
                    zmq::message_t payload_frame(fwd_payload.begin(), fwd_payload.end());
                    peers[target_node].dealer->send(type_frame, zmq::send_flags::sndmore);
                    peers[target_node].dealer->send(payload_frame, zmq::send_flags::none);
                }
                break;
            }

            default: {
                std::cout << "Unknown message type. " << message << std::endl;
                break;
            }
        }
    }

    return 0;
}
