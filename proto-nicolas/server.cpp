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

std::vector<PeerInfo> peers;
std::mutex peers_mutex;
int my_node_id;
int num_nodes;

// Foreground redirections en attente : target_node_id -> client_sender_id
std::map<int, std::string> pending_redirections;

// Background redirections en attente : target_node_id -> client_sender_id
// Effacé seulement quand RemoteJobFinished arrive (pas à RemoteReturnValue)
std::map<int, std::string> pending_bg_redirections;

// Background jobs locaux : local_pid -> {global_pid, client_sender_id, is_remote}
// is_remote = true si le job vient d'un RemoteCommand
std::mutex bg_mutex;
struct BgJob {
    uint32_t global_pid;
    std::string sender_id;  // client_id si local, node_id si remote
    bool is_remote;
};
std::map<pid_t, BgJob> background_jobs;

std::string getLoadAverage() {
    std::ifstream load_file("/proc/loadavg");
    if (!load_file.is_open()) return "0.0";
    std::string line;
    std::getline(load_file, line);
    std::istringstream iss(line);
    std::string firstNumber;
    iss >> firstNumber;
    return firstNumber;
}

void heartbeat_thread(const std::vector<std::string>& addresses, zmq::context_t& context) {
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
                uint8_t type_byte = static_cast<uint8_t>(MessageType::Heartbeat);
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

// Format notification watchdog → receive loop : "global_pid|sender_id|is_remote"
void watchdog_thread(zmq::context_t& context) {
    zmq::socket_t notif(context, zmq::socket_type::pair);
    notif.connect("inproc://jobdone");

    while (true) {
        int status;
        pid_t pid = waitpid(-1, &status, 0);
        if (pid <= 0) continue;

        bg_mutex.lock();
        auto it = background_jobs.find(pid);
        if (it == background_jobs.end()) {
            bg_mutex.unlock();
            continue;
        }
        BgJob job = it->second;
        background_jobs.erase(it);
        bg_mutex.unlock();

        std::string msg = std::to_string(job.global_pid) + "|"
                        + job.sender_id + "|"
                        + (job.is_remote ? "1" : "0");
        zmq::message_t m(msg.begin(), msg.end());
        notif.send(m, zmq::send_flags::none);
    }
}

static void broadcast_load() {
    std::string new_load = getLoadAverage();
    for (int j = 0; j < num_nodes; ++j) {
        if (j == my_node_id) continue;
        peers_mutex.lock();
        Liveness st = peers[j].status;
        peers_mutex.unlock();
        if (st != Liveness::Alive) continue;
        uint8_t hb_byte = static_cast<uint8_t>(MessageType::Heartbeat);
        zmq::message_t hb_type_frame(sizeof(uint8_t));
        memcpy(hb_type_frame.data(), &hb_byte, sizeof(uint8_t));
        zmq::message_t hb_payload(new_load.begin(), new_load.end());
        peers[j].dealer->send(hb_type_frame, zmq::send_flags::sndmore);
        peers[j].dealer->send(hb_payload, zmq::send_flags::none);
    }
}

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

static std::pair<uint32_t, std::string> fork_and_exec(
    const std::string& message, bool background,
    const std::string& sender_id, bool is_remote)
{
    std::string cmd_str = background ? message.substr(0, message.size() - 2) : message;
    auto tokens = split_command(cmd_str);
    if (tokens.empty()) return {0, "error: empty command"};

    int pipefd[2];
    if (pipe(pipefd) < 0) return {0, "error: pipe failed"};

    pid_t pid = fork();
    if (pid < 0) {
        close(pipefd[0]);
        close(pipefd[1]);
        return {0, "error: fork failed"};
    }

    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1], STDOUT_FILENO);
        dup2(pipefd[1], STDERR_FILENO);
        close(pipefd[1]);
        std::vector<char*> exec_argv;
        for (auto& t : tokens) exec_argv.push_back(t.data());
        exec_argv.push_back(nullptr);
        execvp(exec_argv[0], exec_argv.data());
        _exit(127);
    }

    close(pipefd[1]);
    uint32_t global_pid = (static_cast<uint32_t>(my_node_id) << 16) | static_cast<uint32_t>(pid);

    broadcast_load();

    std::string output;
    if (!background) {
        char buf[4096];
        ssize_t n;
        while ((n = read(pipefd[0], buf, sizeof(buf))) > 0)
            output.append(buf, n);
        waitpid(pid, nullptr, 0);
    } else {
        bg_mutex.lock();
        background_jobs[pid] = {global_pid, sender_id, is_remote};
        bg_mutex.unlock();
    }
    close(pipefd[0]);

    return {global_pid, output};
}

static std::string build_response(uint32_t global_pid, const std::string& output, bool background) {
    if (!global_pid) return "error: fork failed";
    if (background)
        return "global_pid:" + std::to_string(global_pid);
    return "global_pid:" + std::to_string(global_pid) + "\n" + output;
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <node_id>" << std::endl;
        return 1;
    }

    my_node_id = std::stoi(argv[1]);

    const std::string config_path = "./config_servers.txt";
    std::ifstream file(config_path);
    if (!file.is_open()) { std::cerr << "Failed to open config file\n"; return 1; }

    std::vector<std::string> addresses;
    std::string line;
    while (std::getline(file, line))
        if (!line.empty()) addresses.push_back(line);

    num_nodes = addresses.size();
    if (my_node_id < 0 || my_node_id >= num_nodes) { std::cerr << "Invalid node_id\n"; return 1; }

    std::cout << "Total nodes: " << num_nodes << std::endl;

    zmq::context_t context(1);
    zmq::socket_t router(context, zmq::socket_type::router);
    std::string identity = std::to_string(my_node_id);
    router.set(zmq::sockopt::routing_id, identity);
    router.set(zmq::sockopt::linger, 0);
    router.bind(addresses[my_node_id]);
    std::cout << "Node " << my_node_id << " bound to " << addresses[my_node_id] << std::endl;

    zmq::socket_t pair_pull(context, zmq::socket_type::pair);
    pair_pull.bind("inproc://jobdone");

    peers.resize(num_nodes);
    for (int j = 0; j < num_nodes; ++j) {
        if (j == my_node_id) continue;
        peers[j].dealer = std::make_unique<zmq::socket_t>(context, zmq::socket_type::dealer);
        peers[j].dealer->set(zmq::sockopt::routing_id, identity);
        peers[j].dealer->set(zmq::sockopt::linger, 0);
        peers[j].dealer->connect(addresses[j]);

        std::string payload = getLoadAverage();
        uint8_t type_byte = static_cast<uint8_t>(MessageType::Heartbeat);
        zmq::message_t type_frame(sizeof(uint8_t));
        memcpy(type_frame.data(), &type_byte, sizeof(uint8_t));
        zmq::message_t payload_frame(payload.begin(), payload.end());
        peers[j].dealer->send(type_frame, zmq::send_flags::sndmore);
        peers[j].dealer->send(payload_frame, zmq::send_flags::none);
        std::cout << "Connected to node " << j << " and sent load: " << payload << std::endl;
    }

    std::thread hb_thread(heartbeat_thread, std::cref(addresses), std::ref(context));
    std::thread wd_thread(watchdog_thread, std::ref(context));

    while (true) {
        zmq::pollitem_t items[] = {
            { static_cast<void*>(router),    0, ZMQ_POLLIN, 0 },
            { static_cast<void*>(pair_pull), 0, ZMQ_POLLIN, 0 }
        };
        zmq::poll(items, 2, -1);

        // ---- Watchdog : job background terminé ----
        if (items[1].revents & ZMQ_POLLIN) {
            zmq::message_t notif;
            pair_pull.recv(notif);
            std::string msg(static_cast<char*>(notif.data()), notif.size());

            // Format : "global_pid|sender_id|is_remote"
            size_t sep1 = msg.find('|');
            size_t sep2 = msg.find('|', sep1 + 1);
            uint32_t global_pid = std::stoul(msg.substr(0, sep1));
            std::string sender_id = msg.substr(sep1 + 1, sep2 - sep1 - 1);
            bool is_remote = msg.substr(sep2 + 1) == "1";

            std::string gpid_payload = "global_pid:" + std::to_string(global_pid);

            if (!is_remote) {
                // Job local — notifier directement le client
                uint8_t resp_byte = static_cast<uint8_t>(MessageType::JobFinished);
                zmq::message_t resp_type_frame(sizeof(uint8_t));
                memcpy(resp_type_frame.data(), &resp_byte, sizeof(uint8_t));
                zmq::message_t resp_payload_frame(gpid_payload.begin(), gpid_payload.end());
                zmq::message_t client_frame(sender_id.begin(), sender_id.end());
                router.send(client_frame, zmq::send_flags::sndmore);
                router.send(resp_type_frame, zmq::send_flags::sndmore);
                router.send(resp_payload_frame, zmq::send_flags::none);
                std::cout << "[WD] Local job " << global_pid << " done, notified " << sender_id << std::endl;
            } else {
                // Job remote — envoyer RemoteJobFinished au nœud source
                int source_node = std::stoi(sender_id);
                uint8_t resp_byte = static_cast<uint8_t>(MessageType::RemoteJobFinished);
                zmq::message_t resp_type_frame(sizeof(uint8_t));
                memcpy(resp_type_frame.data(), &resp_byte, sizeof(uint8_t));
                zmq::message_t resp_payload_frame(gpid_payload.begin(), gpid_payload.end());
                peers[source_node].dealer->send(resp_type_frame, zmq::send_flags::sndmore);
                peers[source_node].dealer->send(resp_payload_frame, zmq::send_flags::none);
                std::cout << "[WD] Remote job " << global_pid << " done, notified node " << source_node << std::endl;
            }
        }

        // ---- Message réseau ----
        if (items[0].revents & ZMQ_POLLIN) {
            zmq::message_t sender, type_frame, payload_frame;
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
                        ? std::stoi(sender_id.substr(10)) : std::stoi(sender_id);
                    peers_mutex.lock();
                    Liveness peer_status = peers[indice].status;
                    peers_mutex.unlock();
                    if (peer_status == Liveness::Dead) {
                        std::cout << "Peer " << sender_id << " came back to life but ignoring." << std::endl;
                        break;
                    }
                    peers_mutex.lock();
                    peers[indice].load = std::stof(message);
                    peers[indice].status = Liveness::Alive;
                    peers[indice].last_heartbeat = std::chrono::steady_clock::now();
                    peers_mutex.unlock();
                    break;
                }

                case MessageType::ClientCommand: {
                    float my_load = std::stof(getLoadAverage());//999.0f si on veut tester l'envoi de ma charge sur d'autre machine
                    peers_mutex.lock();
                    float global_charge = get_global_load(peers);
                    int target = select_target(my_node_id, peers);
                    peers_mutex.unlock();

                    if (my_load > global_charge * 1.5 && target != my_node_id) {
                        std::random_device rd;
                        std::mt19937 gen(rd());
                        std::uniform_int_distribution<> delay(0, 300);
                        std::this_thread::sleep_for(std::chrono::milliseconds(delay(gen)));

                        peers_mutex.lock();
                        global_charge = get_global_load(peers);
                        target = select_target(my_node_id, peers);
                        peers_mutex.unlock();

                        if (target != my_node_id) {
                            bool background = !message.empty() && message.back() == '&';
                            std::cout << "[LB] Redirecting to node " << target
                                      << (background ? " (bg)" : " (fg)") << "\n";

                            if (background) {
                                // Stocker dans pending_bg_redirections
                                pending_bg_redirections[target] = sender_id;
                            } else {
                                pending_redirections[target] = sender_id;
                            }

                            uint8_t remote_byte = static_cast<uint8_t>(MessageType::RemoteCommand);
                            zmq::message_t remote_type_frame(sizeof(uint8_t));
                            memcpy(remote_type_frame.data(), &remote_byte, sizeof(uint8_t));
                            zmq::message_t remote_payload(message.begin(), message.end());
                            peers[target].dealer->send(remote_type_frame, zmq::send_flags::sndmore);
                            peers[target].dealer->send(remote_payload, zmq::send_flags::none);
                            break;
                        }
                    }

                    {
                        bool background = !message.empty() && message.back() == '&';
                        auto [global_pid, output] = fork_and_exec(message, background, sender_id, false);
                        std::string response_payload = build_response(global_pid, output, background);

                        uint8_t resp_byte = static_cast<uint8_t>(MessageType::ClientReturnValue);
                        zmq::message_t resp_type_frame(sizeof(uint8_t));
                        memcpy(resp_type_frame.data(), &resp_byte, sizeof(uint8_t));
                        zmq::message_t resp_payload_frame(response_payload.begin(), response_payload.end());
                        router.send(sender, zmq::send_flags::sndmore);
                        router.send(resp_type_frame, zmq::send_flags::sndmore);
                        router.send(resp_payload_frame, zmq::send_flags::none);
                    }
                    break;
                }

                case MessageType::RemoteCommand: {
                    bool background = !message.empty() && message.back() == '&';
                    // sender_id = node source (ex: "0")
                    auto [global_pid, output] = fork_and_exec(message, background, sender_id, true);
                    std::string response_payload = build_response(global_pid, output, background);

                    int source_node = std::stoi(sender_id);
                    uint8_t resp_byte = static_cast<uint8_t>(MessageType::RemoteReturnValue);
                    zmq::message_t resp_type_frame(sizeof(uint8_t));
                    memcpy(resp_type_frame.data(), &resp_byte, sizeof(uint8_t));
                    zmq::message_t resp_payload_frame(response_payload.begin(), response_payload.end());
                    peers[source_node].dealer->send(resp_type_frame, zmq::send_flags::sndmore);
                    peers[source_node].dealer->send(resp_payload_frame, zmq::send_flags::none);
                    break;
                }

                case MessageType::RemoteReturnValue: {
    int source_node = std::stoi(sender_id);

    // Chercher foreground d'abord
    auto it = pending_redirections.find(source_node);
    if (it != pending_redirections.end()) {
        std::string client_id = it->second;
        pending_redirections.erase(it);

        uint8_t resp_byte = static_cast<uint8_t>(MessageType::ClientReturnValue);
        zmq::message_t resp_type_frame(sizeof(uint8_t));
        memcpy(resp_type_frame.data(), &resp_byte, sizeof(uint8_t));
        zmq::message_t resp_payload_frame(message.begin(), message.end());
        zmq::message_t client_frame(client_id.begin(), client_id.end());
        router.send(client_frame, zmq::send_flags::sndmore);
        router.send(resp_type_frame, zmq::send_flags::sndmore);
        router.send(resp_payload_frame, zmq::send_flags::none);
        break;
    }

    // Sinon chercher background — NE PAS effacer, on attend encore RemoteJobFinished
    auto it2 = pending_bg_redirections.find(source_node);
    if (it2 != pending_bg_redirections.end()) {
        std::string client_id = it2->second;

        uint8_t resp_byte = static_cast<uint8_t>(MessageType::ClientReturnValue);
        zmq::message_t resp_type_frame(sizeof(uint8_t));
        memcpy(resp_type_frame.data(), &resp_byte, sizeof(uint8_t));
        zmq::message_t resp_payload_frame(message.begin(), message.end());
        zmq::message_t client_frame(client_id.begin(), client_id.end());
        router.send(client_frame, zmq::send_flags::sndmore);
        router.send(resp_type_frame, zmq::send_flags::sndmore);
        router.send(resp_payload_frame, zmq::send_flags::none);
        break;
    }

    std::cout << "[ERR] RemoteReturnValue from " << sender_id << " but no pending client\n";
    break;
}

                case MessageType::RemoteJobFinished: {
                    // Job background distant terminé — retrouver le client via pending_bg_redirections
                    int source_node = std::stoi(sender_id);
                    auto it = pending_bg_redirections.find(source_node);
                    if (it == pending_bg_redirections.end()) {
                        std::cout << "[ERR] RemoteJobFinished from " << sender_id << " but no pending bg client\n";
                        break;
                    }
                    std::string client_id = it->second;
                    pending_bg_redirections.erase(it);

                    // Transmettre JobFinished au vrai client
                    uint8_t resp_byte = static_cast<uint8_t>(MessageType::JobFinished);
                    zmq::message_t resp_type_frame(sizeof(uint8_t));
                    memcpy(resp_type_frame.data(), &resp_byte, sizeof(uint8_t));
                    zmq::message_t resp_payload_frame(message.begin(), message.end());
                    zmq::message_t client_frame(client_id.begin(), client_id.end());
                    router.send(client_frame, zmq::send_flags::sndmore);
                    router.send(resp_type_frame, zmq::send_flags::sndmore);
                    router.send(resp_payload_frame, zmq::send_flags::none);
                    std::cout << "[WD] RemoteJobFinished forwarded to " << client_id << std::endl;
                    break;
                }

                case MessageType::ClientHandshake: {
                    uint8_t resp_byte = static_cast<uint8_t>(MessageType::ClientAcknowledgement);
                    zmq::message_t resp_type_frame(sizeof(uint8_t));
                    memcpy(resp_type_frame.data(), &resp_byte, sizeof(uint8_t));
                    zmq::message_t resp_payload_frame;
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
                        std::string fwd = std::to_string(target_node) + ":" + std::to_string(sig) + ":" + std::to_string(lpid);
                        uint8_t type_byte = static_cast<uint8_t>(MessageType::RemoteKill);
                        zmq::message_t tf(sizeof(uint8_t));
                        memcpy(tf.data(), &type_byte, sizeof(uint8_t));
                        zmq::message_t pf(fwd.begin(), fwd.end());
                        peers[target_node].dealer->send(tf, zmq::send_flags::sndmore);
                        peers[target_node].dealer->send(pf, zmq::send_flags::none);
                    }
                    break;
                }

                default:
                    std::cout << "Unknown message type. " << message << std::endl;
                    break;
            }
        }
    }

    return 0;
}
