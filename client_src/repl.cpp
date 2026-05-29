#include <iostream>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
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

//socket de comunication dealer
static zmq::context_t zmq_ctx{1};
static zmq::socket_t  zmq_dealer{zmq_ctx, zmq::socket_type::dealer};

// true entre l'envoi d'une ClientCommand foreground et la réception de ClientReturnValue
// le handler SIGINT s'en sert pour savoir s'il doit envoyer Stop ou juste afficher ^C
static volatile bool fg_running = false;

/* ---------- forward declaration ---------- */
static void execute_command(const Command& cmd);

/* ---------- helpers ZeroMQ ---------- */

// envoi 2 frames : type + payload
static void zmq_send(MessageType type, const std::string& payload)
{
    uint8_t type_byte = static_cast<uint8_t>(type);
    zmq::message_t type_frame(&type_byte, sizeof(type_byte));
    zmq::message_t payload_frame(payload.begin(), payload.end());
    zmq_dealer.send(type_frame,    zmq::send_flags::sndmore);
    zmq_dealer.send(payload_frame, zmq::send_flags::none);
}

// recv bloquant — relance automatiquement si interrompu par SIGINT (EINTR)
//catch nécessaire car une fois reveiller pour traiter le signal 
//on ne reprend pas son attente
static std::pair<MessageType, std::string> zmq_recv()
{
    while (true) {
        try {
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
        } catch (const zmq::error_t& e) {
            if (e.num() == EINTR) continue; // interrompu par le signal, on relance
            throw;
        }
    }
}

/* ---------- SIGINT handler ---------- */

// on ne tue pas le processus directement depuis ici — on ne connaît pas son pid
// le server maintient background_jobs et sait quel job foreground appartient à ce client
static void sigint_handler(int)
{
    if (fg_running) {
        zmq_send(MessageType::Stop, "");
    }
    // si pas de job fg en cours, readline se débrouille (affiche ^C et remet le prompt)
}

/* ---------- handlers asynchrones ---------- */

// notification de fin de job background — peut arriver n'importe quand
// entre deux commandes ou pendant un wait()
//(c'est ici qu'il aurait fallur trafiquer des choses pour mettre le resultat
//dans flux demander par l'application tournant sur le dbash
//GlobalPid: [numéro]\n[Le résultat textuel de la commande...] payload qu'on reçoit
static void handle_job_finished(const std::string& payload)
{
    uint32_t gpid = std::stoul(payload.substr(11, payload.find('\n') - 11));
    std::string output = payload.find('\n') != std::string::npos
        ? payload.substr(payload.find('\n') + 1) : "";

    std::lock_guard<std::mutex> lock(job_mutex);//useless un peu
    for (Job& j : job_table) {
        if (j.global_pid == gpid) {
            j.finished = true;
            std::cout << "\n[done] " << j.command << " (pid " << gpid << ")\n";
            if (!output.empty()) std::cout << output;
            break;
        }
    }
}

// un nœud est mort — on marque ses jobs comme failed et on les relance ailleurs
// le load balancer choisira un nœud disponible parmi les survivants
static void handle_node_dead(int dead_node)
{
    std::vector<std::string> relancer;
    {
        std::lock_guard<std::mutex> lock(job_mutex);
        for (Job& j : job_table) {
            if (j.node_id == dead_node && !j.finished) {
                j.finished = true;
                j.failed   = true;
                relancer.push_back(j.command + " &");
            }
        }
    }
    for (const std::string& cmd_str : relancer) {
        Command cmd = parse_line(cmd_str);
        execute_command(cmd);
    }
}

/* ---------- check notifications ---------- */

// polling non-bloquant (timeout 10ms) pour vider les notifications en attente
// appelé dans ps, wait, et après chaque commande
//ici on ne veut pas que le client s'endorme si le réseau est vide.
//donc poll.
static void check_job_notifications()
{
    zmq::pollitem_t items[] = {//j'indique ce que je veux surveiller
        { static_cast<void*>(zmq_dealer), 0, ZMQ_POLLIN, 0 }
    };
    int rc = zmq::poll(items, 1, std::chrono::milliseconds(10));
    while (rc > 0 && (items[0].revents & ZMQ_POLLIN)) {
        auto [type, payload] = zmq_recv();
        if (type == MessageType::JobFinished) {
            handle_job_finished(payload);
        } else if (type == MessageType::NodeDead) {
            handle_node_dead(std::stoi(payload));
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
    check_job_notifications(); // on vide les notifications avant d'afficher
    std::lock_guard<std::mutex> lock(job_mutex);
    std::cout << "global_pid   node   command   status   failed\n";
    for (Job& j : job_table) {
        if (!j.finished) {
            std::cout << j.global_pid << "   " << j.node_id << "   "
                      << j.command   << "   " << j.finished << "   " << j.failed << "\n";
        }
    }
}

static void builtin_wait_all()
{
    // boucle jusqu'à ce que tous les jobs background soient terminés
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
    int      sig  = std::stoi(cmd.args[1].substr(1)); // "-9" → 9
    uint32_t gpid = std::stoul(cmd.args[2]);

    Job* j = find_job(gpid);
    if (j == nullptr) {
        std::cout << "kill: pid " << gpid << " introuvable\n";
        return;
    }
    if (j->node_id == my_node_id) {
        // job local : kill direct
        kill(j->local_pid, sig);
    } else {
        // job distant : on envoie RemoteKill au server
        // lui se charge de le router vers le bon nœud
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

    // reconstruction de la commande en string pour l'envoyer au server
    std::string command_str;
    for (const auto& arg : cmd.args) {
        if (!command_str.empty()) command_str += " ";
        command_str += arg;
    }
    if (cmd.background) command_str += " &";

    zmq_send(MessageType::ClientCommand, command_str);

    // on arme fg_running avant le recv bloquant
    // comme ça le handler SIGINT peut envoyer Stop si Ctrl+C arrive pendant l'attente
    if (!cmd.background) {
        fg_running = true;
    }

    // boucle de réception — on ignore les notifications asynchrones
    // (JobFinished, NodeDead) et on attend ClientReturnValue
    //pour bien bloquer le shell 
    MessageType type;
    std::string payload;
    while (true) {
        auto [t, p] = zmq_recv();
        if (t == MessageType::JobFinished) {
            handle_job_finished(p);
            continue;
        }
        if (t == MessageType::NodeDead) {
            handle_node_dead(std::stoi(p));
            continue;
        }
        type = t;
        payload = p;
        break;
    }
    //bon meme avec & on bloque quand meme quelque ms .
    if (!cmd.background) {
        fg_running = false;
    }

    if (type == MessageType::ClientReturnValue) {
        if (payload.substr(0, 11) == "global_pid:") {
            size_t newline = payload.find('\n');
            uint32_t gpid = std::stoul(payload.substr(11,
                newline != std::string::npos ? newline - 11 : std::string::npos));
            std::string output = (newline != std::string::npos)
                ? payload.substr(newline + 1) : "";

            // enregistrement dans la job_table locale
            Job j;
            j.local_pid  = gpid & 0xFFFF;      // 16 bits de poids faible
            j.global_pid = gpid;
            j.node_id    = gpid >> 16;          // 16 bits de poids fort
            j.command    = cmd.background
                ? command_str.substr(0, command_str.size() - 2)
                : command_str;
            j.background = cmd.background;
            j.finished   = !cmd.background;    // fg déjà terminé, bg encore en cours
            j.failed     = false;
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

    //on creait une liste de server aux quel on peut se connecter 
    std::vector<std::string> endpoints;
    std::string line;
    while (std::getline(file, line))
        if (!line.empty()) endpoints.push_back(line);

    if (node_id < 0 || node_id >= static_cast<int>(endpoints.size())) {
        std::cerr << "Invalid node_id\n";
        exit(1);
    }

    // identity basée sur getpid() → unique même si plusieurs dbash sur la même machine
    std::string identity = "client_" + std::to_string(getpid());
    zmq_dealer.set(zmq::sockopt::routing_id, identity);
    zmq_dealer.set(zmq::sockopt::linger, 0);
    zmq_dealer.connect(endpoints[node_id]);

    // handshake initial — le server enregistre notre routing_id dans connected_clients
    zmq_send(MessageType::ClientHandshake, "");
    auto [ack_type, ack_payload] = zmq_recv();
    if (ack_type != MessageType::ClientAcknowledgement) {
        std::cerr << "Handshake failed\n";
        exit(1);
    }

    // installation du handler SIGINT après le handshake
    // avant ça on ne peut pas envoyer Stop (pas encore connecté)
    struct sigaction sa{};
    sa.sa_handler = sigint_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, nullptr);

    std::cout << "D-bash — shell réparti\nTape 'exit' ou Ctrl+D pour quitter.\n";

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

/*
std::thread notif_thread([&]() {
    while (true) {
        check_job_notifications();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
});
notif_thread.detach();*///
//detach() dit au thread de tourner de façon indépendante 
//j'ai enlever ce mecanisme car j'ai pas envie d'avoir des
//notification qui pop de nulle part. 