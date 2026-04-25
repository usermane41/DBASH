#include <iostream>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <fstream>
#include <vector>
#include <zmq.hpp>
#include "repl.hpp"
#include "parser.hpp"
#include "job_table.hpp"
#include "load_balancer.hpp"
#include "message_types.hpp"

static constexpr const char* PROMPT = "dbash> ";

static zmq::context_t zmq_ctx{1};
static zmq::socket_t  zmq_dealer{zmq_ctx, zmq::socket_type::dealer};

/* ---------- helpers ZeroMQ ---------- */

static void zmq_send(MessageType type, const std::string& payload)
{
    uint8_t type_byte = static_cast<uint8_t>(type);
    zmq::message_t type_frame(&type_byte, sizeof(type_byte));
    zmq::message_t payload_frame(payload.begin(), payload.end());
    zmq_dealer.send(type_frame,    zmq::send_flags::sndmore);
    zmq_dealer.send(payload_frame, zmq::send_flags::none);
}

static std::pair<MessageType, std::string> zmq_recv()
{
    zmq::message_t type_frame;
    zmq::message_t payload_frame;
    zmq_dealer.recv(type_frame);
    zmq_dealer.recv(payload_frame);
    MessageType type = static_cast<MessageType>(
        *static_cast<uint8_t*>(type_frame.data())
    );
    std::string payload(
        static_cast<char*>(payload_frame.data()),
        payload_frame.size()
    );
    return {type, payload};
}

// Traite un JobFinished
static void handle_job_finished(const std::string& payload)
{
    uint32_t gpid = std::stoul(payload.substr(11));
    std::lock_guard<std::mutex> lock(job_mutex);
    for (Job& j : job_table) {
        if (j.global_pid == gpid) {
            j.finished = true;
            std::cout << "\n[done] " << j.command << " (pid " << gpid << ")\n";
            break;
        }
    }
}

// Vérifie si le serveur a envoyé des notifications JobFinished en attente
static void check_job_notifications()
{
    zmq::pollitem_t items[] = {
        { static_cast<void*>(zmq_dealer), 0, ZMQ_POLLIN, 0 }
    };
    std::cout << "[DEBUG check] polling...\n";
    int rc = zmq::poll(items, 1, std::chrono::milliseconds(10));
    std::cout << "[DEBUG check] poll returned: " << rc << "\n";
    while (rc > 0 && (items[0].revents & ZMQ_POLLIN)) {
        auto [type, payload] = zmq_recv();
        std::cout << "[DEBUG check] got message type: " << static_cast<int>(type) << "\n";
        if (type == MessageType::JobFinished) {
            handle_job_finished(payload);
        }
        rc = zmq::poll(items, 1, std::chrono::milliseconds(10));
    }
}

/* ---------- built-ins ---------- */

[[noreturn]] static void builtin_exit()
{
    std::cout << "Bye.\n";
    exit(0);
}

static void builtin_ps()
{
    check_job_notifications();
    std::lock_guard<std::mutex> lock(job_mutex);
    std::cout << "global_pid   node   command   status\n";
    for (Job& j : job_table) {
        std::cout << j.global_pid << "   " << j.node_id << "   "
                  << j.command   << "   " << j.finished << "\n";
    }
}

static void builtin_wait_all()
{
    bool finished = false;
    while (!finished) {
        check_job_notifications();
        finished = true;
        {
            std::lock_guard<std::mutex> lock(job_mutex);
            for (Job& j : job_table) {
                if (!j.finished && j.background) {
                    finished = false;
                }
            }
        }
        if (!finished) sleep(1);
    }
}

static void builtin_kill(const Command& cmd)
{
    int      sig  = std::stoi(cmd.args[1].substr(1));
    uint32_t gpid = std::stoul(cmd.args[2]);

    Job* j = find_job(gpid);
    if (j == nullptr) {
        std::cout << "kill: pid " << gpid << " introuvable\n";
        return;
    }
    if (j->node_id == my_node_id) {
        kill(j->local_pid, sig);
    } else {
        std::string payload = std::to_string(j->node_id) + ":"
                            + std::to_string(sig) + ":"
                            + std::to_string(j->local_pid);
        zmq_send(MessageType::RemoteKill, payload);
    }
}

/* ---------- dispatch ---------- */

static bool handle_builtin(const Command& cmd)
{
    const auto& name = cmd.name();
    if (name == "exit") { builtin_exit(); }
    if (name == "ps")   { builtin_ps();       return true; }
    if (name == "wait") { builtin_wait_all(); return true; }
    if (name == "kill") { builtin_kill(cmd);  return true; }
    return false;
}

/* ---------- execute ---------- */

static void execute_command(const Command& cmd)
{
    if (handle_builtin(cmd)) return;

    std::string command_str;
    for (const auto& arg : cmd.args) {
        if (!command_str.empty()) command_str += " ";
        command_str += arg;
    }
    if (cmd.background) command_str += " &";

    zmq_send(MessageType::ClientCommand, command_str);

    // Recevoir la réponse — peut recevoir des JobFinished en chemin
    MessageType type;
    std::string payload;
    while (true) {
        auto [t, p] = zmq_recv();
        if (t == MessageType::JobFinished) {
            handle_job_finished(p);
            continue;
        }
        type = t;
        payload = p;
        break;
    }

    if (type == MessageType::ClientReturnValue) {
        if (payload.substr(0, 11) == "global_pid:") {
            size_t newline = payload.find('\n');
            uint32_t gpid = std::stoul(payload.substr(11,
                newline != std::string::npos ? newline - 11 : std::string::npos));
            std::string output = (newline != std::string::npos)
                ? payload.substr(newline + 1) : "";

            Job j;
            j.local_pid  = gpid & 0xFFFF;
            j.global_pid = gpid;
            j.node_id    = gpid >> 16;
            j.command    = cmd.name();
            j.background = cmd.background;
            j.finished   = !cmd.background;
            add_job(j);

            if (!output.empty()) std::cout << output;
        } else {
            std::cout << payload;
        }
    }

    check_job_notifications();
}

/* ---------- boucle principale ---------- */

void repl_run(int node_id)
{
    const std::string config_path = "./config_servers.txt";
    std::ifstream file(config_path);
    if (!file) { std::cerr << "Failed to open config file\n"; exit(1); }

    std::vector<std::string> endpoints;
    std::string line;
    while (std::getline(file, line))
        if (!line.empty()) endpoints.push_back(line);

    if (node_id < 0 || node_id >= static_cast<int>(endpoints.size())) {
        std::cerr << "Invalid node_id\n";
        exit(1);
    }

    std::string identity = "client_" + std::to_string(node_id);
    zmq_dealer.set(zmq::sockopt::routing_id, identity);
    zmq_dealer.set(zmq::sockopt::linger, 0);
    zmq_dealer.connect(endpoints[node_id]);

    zmq_send(MessageType::ClientHandshake, "");
    auto [ack_type, ack_payload] = zmq_recv();
    if (ack_type != MessageType::ClientAcknowledgement) {
        std::cerr << "Handshake failed\n";
        exit(1);
    }

    char* raw;
    while ((raw = readline(PROMPT)) != nullptr) {
        std::string input(raw);
        if (!input.empty()) add_history(raw);
        free(raw);

        Command cmd = parse_line(input);
        if (cmd.empty()) continue;
        execute_command(cmd);
    }

    std::cout << "\n";
    builtin_exit();
}
