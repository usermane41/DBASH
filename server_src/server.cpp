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
#include <functional>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <load_balancer.hpp>
#include <random>
#include <map>
#include <set>
#include <atomic>

// état global du cluster — partagé entre le receive loop et le heartbeat thread
// peers_mutex protège tous les accès à peers[]
std::vector<PeerInfo> peers;
std::mutex peers_mutex;
int my_node_id;
int num_nodes;

// clients connectés à ce nœud, identifiés par leur routing_id zmq
std::set<std::string> connected_clients;

// quand on redirige une commande vers un autre nœud, on mémorise
// quel client attend la réponse pour pouvoir la lui renvoyer
// fg : map car un seul foreground redirigé par nœud cible à la fois
// bg : multimap car plusieurs background peuvent partir vers le même nœud
std::map<int, std::string>      pending_redirections;
std::multimap<int, std::string> pending_bg_redirections;

// jobs en cours d'exécution sur ce nœud (fg et bg confondus et oui le nom est pas très explicite)
// bg_mutex protège background_jobs et les accès depuis le watchdog
std::mutex bg_mutex;
struct BgJob {
    uint32_t    global_pid; //qui c'est l'idiot qui croyait qu'un pid sa tenait sur 16bit ...
    std::string sender_id;    // routing_id du client si local, node_id si remote
    bool        is_remote;
    bool        is_foreground; // détermine ce que le watchdog enverra à la fin
    int         pipe_read_fd;  // lecture de l'output du processus enfant
};
std::map<pid_t, BgJob> background_jobs;

// charge CPU courante — mise à jour par cpu_monitor_thread toutes les 200ms
// atomic pour que le receive loop puisse lire sans mutex
static std::atomic<float> cpu_usage{0.0f};

// ─── ZMQ helpers ────────────────────────────────────────────
// on factorise l'envoi zmq car le pattern 2 frames (type + payload) revient partout

//faut creer un zmq::message 
//memcpy nécessaire cette structure zmq nécessite de passer par son 
//pointeur de donner et y copier les octets manuellement. 
//(possede un constructeur intelligent donc sais mesurer la taille
//tout seul et copier les data directement.
static void zmq_send_2(zmq::socket_t& sock,
                        MessageType type,
                        const std::string& payload,
                        zmq::send_flags flags = zmq::send_flags::none)
{
    uint8_t type_byte = static_cast<uint8_t>(type);
    zmq::message_t type_frame(sizeof(uint8_t));//nécessaire pour 
    memcpy(type_frame.data(), &type_byte, sizeof(uint8_t));
    zmq::message_t payload_frame(payload.begin(), payload.end());
    sock.send(type_frame, zmq::send_flags::sndmore);
    sock.send(payload_frame, flags);
}

// via le ROUTER : 3 frames obligatoires — identity + type + payload
//Le Routeur contrairement au dealer a besoin de savoir a qui envoyer lui.
static void zmq_send_to_client(zmq::socket_t& router,
                                const std::string& client_id,
                                MessageType type,
                                const std::string& payload)
{
    zmq::message_t client_frame(client_id.begin(), client_id.end());
    uint8_t type_byte = static_cast<uint8_t>(type);
    zmq::message_t type_frame(sizeof(uint8_t));
    memcpy(type_frame.data(), &type_byte, sizeof(uint8_t));
    zmq::message_t payload_frame(payload.begin(), payload.end());
    router.send(client_frame,  zmq::send_flags::sndmore);
    router.send(type_frame,    zmq::send_flags::sndmore);
    router.send(payload_frame, zmq::send_flags::none);
}

// via le DEALER du peer — pas d'identity frame ici, c'est le DEALER qui l'ajoute
static void zmq_send_to_peer(int peer_idx,
                              MessageType type,
                              const std::string& payload)
{
    zmq_send_2(*peers[peer_idx].dealer, type, payload);
}

// ─── /proc/stat ─────────────────────────────────────────────
// contrairement à /proc/loadavg (moyenne sur 1 min, très lente à réagir),
// /proc/stat donne des compteurs cumulés depuis le boot
// on lit deux fois avec 200ms d'écart et on calcule le delta → usage instantané

static void read_stat(long& total, long& idle) {
    std::ifstream f("/proc/stat");
    std::string cpu;
    long u, n, s, i, w, x, y, z;
    f >> cpu >> u >> n >> s >> i >> w >> x >> y >> z;
    idle  = i;
    total = u+n+s+i+w+x+y+z;
}

void cpu_monitor_thread() {
    while (true) {
        long total1, idle1, total2, idle2;
        read_stat(total1, idle1);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        read_stat(total2, idle2);
        // usage = fraction du temps où le CPU travaillait vraiment
        float usage = 1.0f - (float)(idle2-idle1) / (float)(total2-total1);
        cpu_usage.store(usage);//PSCR aura bien servi on tout cas?
    }
}

std::string getLoadAverage() {
    return std::to_string(cpu_usage.load());
}

// ─── Heartbeat thread ───────────────────────────────────────
// ce thread a ses propres dealers (pas le droit de toucher ceux du receive loop,
// zmq n'est pas thread-safe)
// double rôle : diffuser notre charge + détecter les nœuds morts
//on respecte la contrainte de non-partage des sockets ZeroMQ entre threads
void heartbeat_thread(const std::vector<std::string>& addresses, zmq::context_t& context) {
    std::vector<zmq::socket_t> dealers(num_nodes);
    //initialisation de toutes les sockets dealer 
     
    for (int j = 0; j < num_nodes; ++j) {
        if (j == my_node_id) continue;
        dealers[j] = zmq::socket_t(context, zmq::socket_type::dealer);
        std::string identity = "heartbeat_" + std::to_string(my_node_id);
        dealers[j].set(zmq::sockopt::routing_id, identity);
        dealers[j].set(zmq::sockopt::linger, 0);
        dealers[j].connect(addresses[j]);
        std::cout << "[HB] Connected to peer " << j << std::endl;
    }
    // communication interne dans la RAM entre deux threads du même programme
    // socket PAIR  pour notifier le receive loop qu'un nœud est mort
    // on ne peut pas envoyer directement via le ROUTER (pas thread-safe)
    zmq::socket_t notif_dead(context, zmq::socket_type::pair);
    notif_dead.connect("inproc://nodedead");

    while (true) {
        auto now = std::chrono::steady_clock::now();
        std::string payload = getLoadAverage();

        for (int j = 0; j < num_nodes; ++j) {
            if (j == my_node_id) continue;

            peers_mutex.lock();
            Liveness peer_status = peers[j].status;
            auto     last_hb     = peers[j].last_heartbeat;
            peers_mutex.unlock();
            //verifier si toujours vivant
            if (peer_status == Liveness::Alive) {
                auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_hb);
                if (elapsed.count() > 10) {
                    // plus de nouvelles depuis 10s → on le déclare mort
                    peers_mutex.lock();
                    peers[j].status = Liveness::Dead;
                    peers_mutex.unlock();
                    std::cout << "[HB] Peer " << j << " marked DEAD" << std::endl;
                    std::string msg = std::to_string(j);
                    zmq::message_t m(msg.begin(), msg.end());
                    notif_dead.send(m, zmq::send_flags::none);
                    continue;
                }
            }
            //envoyer son heartbeat a j.
            if (peer_status == Liveness::Alive || peer_status == Liveness::Uninitialized) {
                zmq_send_2(dealers[j], MessageType::Heartbeat, payload);
                std::cout << "[HB] Sent to peer " << j << std::endl;
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(3));
    }
}

// ─── Watchdog thread ────────────────────────────────────────
// bloqué sur waitpid(-1) — intercepte la fin de n'importe quel processus enfant
// fg et bg sont traités de la même façon ici, c'est is_foreground qui
// détermine ce que le receive loop fera avec la notification
//
// format de la notification envoyée au receive loop via inproc://jobdone :
// "global_pid|sender_id|is_remote|is_foreground|output"
void watchdog_thread(zmq::context_t& context) {
    //socket PAIR connectée en inproc
    zmq::socket_t notif(context, zmq::socket_type::pair);
    notif.connect("inproc://jobdone");

    while (true) {
        //attend la fin de n'importe quel enfant du server
        int status;
        pid_t pid = waitpid(-1, &status, 0);//-1 vue que l'enfant fessait setgpid
        if (pid <= 0) continue;//mais sert plus a grand la je crois vue que j'ai enlever le setpgid

        //chercher le job en memoire et le supprimer
        bg_mutex.lock();
        auto it = background_jobs.find(pid);
        if (it == background_jobs.end()) { bg_mutex.unlock(); continue; }
        BgJob job = it->second;
        background_jobs.erase(it);
        bg_mutex.unlock();

        // vider le pipe de redirection de stdout qu'on a fait du fils
        // avant de le fermer
        std::string output;
        char buf[4096];
        ssize_t n;
        while ((n = read(job.pipe_read_fd, buf, sizeof(buf))) > 0)
            output.append(buf, n);
        close(job.pipe_read_fd);

        //envoi du message de notification au thread principal
        //qui va renvoyer le tout au bon client
        std::string msg = std::to_string(job.global_pid) + "|"
                        + job.sender_id + "|"
                        + (job.is_remote     ? "1" : "0") + "|"
                        + (job.is_foreground ? "1" : "0") + "|"
                        + output;
        zmq::message_t m(msg.begin(), msg.end());
        notif.send(m, zmq::send_flags::none);
    }
}

// ─── Helpers locaux ─────────────────────────────────────────

// appelé juste après chaque fork pour que les pairs voient notre charge à jour
// évite le honeypot : si on vient de lancer un job lourd, les autres
// sauront qu'on est chargé avant le prochain heartbeat (dans 3s)
static void broadcast_load() {
    std::string new_load = getLoadAverage();
    for (int j = 0; j < num_nodes; ++j) {
        if (j == my_node_id) continue;
        peers_mutex.lock();
        Liveness st = peers[j].status;
        peers_mutex.unlock();
        if (st != Liveness::Alive) continue;
        zmq_send_to_peer(j, MessageType::Heartbeat, new_load);
    }
}

// split la commande en tokens, s'arrête au '&' s'il y en a un
//va nous servir pour le la fonction 
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

// fork + exec non-bloquant — le receive loop ne bloque JAMAIS ici
// tous les jobs (fg et bg) sont enregistrés dans background_jobs
// c'est le watchdog qui détecte la fin et notifie le receive loop
static uint32_t fork_and_exec(const std::string& message, bool background,
                               const std::string& sender_id, bool is_remote)
{
    std::string cmd_str = background ? message.substr(0, message.size() - 2) : message;
    auto tokens = split_command(cmd_str);
    if (tokens.empty()) return 0;

    //pour capturer la sortie de l'enfant;
    int pipefd[2];
    if (pipe(pipefd) < 0) return 0;

    pid_t pid = fork();
    if (pid < 0) { close(pipefd[0]); close(pipefd[1]); return 0; }

    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1], STDOUT_FILENO);
        dup2(pipefd[1], STDERR_FILENO);
        close(pipefd[1]);
        //nécessaire de convertir en char* pour execvp d
        std::vector<char*> exec_argv;
        for (auto& t : tokens) exec_argv.push_back(t.data());
        exec_argv.push_back(nullptr);
        execvp(exec_argv[0], exec_argv.data());
        _exit(127);
    }
    close(pipefd[1]);

    // global_pid = (node_id << 16) | local_pid — unique sur tout le cluster
    uint32_t global_pid = (static_cast<uint32_t>(my_node_id) << 16) | static_cast<uint32_t>(pid);

    broadcast_load();
    //enregistrer pour le watch dog
    bg_mutex.lock();
    background_jobs[pid] = {global_pid, sender_id, is_remote, !background, pipefd[0]};
    bg_mutex.unlock();

    return global_pid;
}

// ─── Main ───────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    //lecture de la configuration
    if (argc != 2) { std::cerr << "Usage: " << argv[0] << " <node_id>\n"; return 1; }

    my_node_id = std::stoi(argv[1]);

    std::ifstream file("./config_servers.txt");
    if (!file.is_open()) { std::cerr << "Failed to open config file\n"; return 1; }

    std::vector<std::string> addresses;
    std::string line;
    while (std::getline(file, line))
        if (!line.empty()) addresses.push_back(line);

    num_nodes = addresses.size();
    if (my_node_id < 0 || my_node_id >= num_nodes) { std::cerr << "Invalid node_id\n"; return 1; }
    std::cout << "Total nodes: " << num_nodes << std::endl;

    zmq::context_t context(1);

    // initialisation du ROUTER principal — seule socket qui reçoit tout (clients + autres nœuds)
    zmq::socket_t router(context, zmq::socket_type::router);
    std::string identity = std::to_string(my_node_id);
    router.set(zmq::sockopt::routing_id, identity);
    router.set(zmq::sockopt::linger, 0);
    router.bind(addresses[my_node_id]);
    std::cout << "Node " << my_node_id << " bound to " << addresses[my_node_id] << std::endl;

    // creations des sockets PAIR inproc — canaux privés entre threads
    // les threads ne peuvent pas toucher le ROUTER directement (zmq pas thread-safe)
    zmq::socket_t pair_pull(context, zmq::socket_type::pair);
    pair_pull.bind("inproc://jobdone");

    zmq::socket_t pair_nodedead(context, zmq::socket_type::pair);
    pair_nodedead.bind("inproc://nodedead");

    // DEALERs vers les autres nœuds + envoi heartbeat initial pour se présenter
    peers.resize(num_nodes);
    for (int j = 0; j < num_nodes; ++j) {
        if (j == my_node_id) continue;
        peers[j].dealer = std::make_unique<zmq::socket_t>(context, zmq::socket_type::dealer);//creation socket
        peers[j].dealer->set(zmq::sockopt::routing_id, identity);//set l'id
        peers[j].dealer->set(zmq::sockopt::linger, 0);//on ne cherche pas à vider les files d'attente réseau
        peers[j].dealer->connect(addresses[j]);//connecte physiquement à l'adresse IP
        zmq_send_to_peer(j, MessageType::Heartbeat, getLoadAverage());
        std::cout << "Connected to node " << j << std::endl;
    }
    //lancer les thread une fois l'infra réseau de prète
    std::thread hb_thread(heartbeat_thread, std::cref(addresses), std::ref(context));
    std::thread wd_thread(watchdog_thread, std::ref(context));
    std::thread cpu_thread(cpu_monitor_thread);

    // ─── Receive loop ────────────────────────────────────────
    // thread principal — seul à toucher router, pair_pull, pair_nodedead
    // zmq::poll surveille les 3 sources simultanément sans bloquer sur l'une d'elles
    while (true) {
        zmq::pollitem_t items[] = {
            { static_cast<void*>(router),        0, ZMQ_POLLIN, 0 },
            { static_cast<void*>(pair_pull),     0, ZMQ_POLLIN, 0 },
            { static_cast<void*>(pair_nodedead), 0, ZMQ_POLLIN, 0 }
        };
        zmq::poll(items, 3, -1);

        // ── Watchdog : job terminé ──────────────────────────
        // le watchdog a détecté la fin d'un processus enfant
        // on parse sa notification et on dispatche selon local/remote et fg/bg
        if (items[1].revents & ZMQ_POLLIN) {
            zmq::message_t notif;
            pair_pull.recv(notif);
            std::string msg(static_cast<char*>(notif.data()), notif.size());

            size_t s1 = msg.find('|'), s2 = msg.find('|', s1+1),
                   s3 = msg.find('|', s2+1), s4 = msg.find('|', s3+1);
            uint32_t    global_pid = std::stoul(msg.substr(0, s1));
            std::string sender_id  = msg.substr(s1+1, s2-s1-1);
            bool        is_remote  = msg.substr(s2+1, s3-s2-1) == "1";
            bool        is_fg      = msg.substr(s3+1, s4-s3-1) == "1";
            std::string output     = msg.substr(s4+1);
            std::string gpid_str   = "global_pid:" + std::to_string(global_pid) + "\n";

            if (!is_remote) {
                // job local : on répond directement au client
                if (is_fg) {
                    zmq_send_to_client(router, sender_id, MessageType::ClientReturnValue, gpid_str + output);
                    std::cout << "[WD] FG job " << global_pid << " done → client " << sender_id << std::endl;
                } else {
                    zmq_send_to_client(router, sender_id, MessageType::JobFinished, gpid_str + output);
                    std::cout << "[WD] BG job " << global_pid << " done → client " << sender_id << std::endl;
                }
            } else {
                // job remote : on renvoie RemoteJobFinished au nœud source
                // lui se chargera de notifier son client
                int source_node = std::stoi(sender_id);
                zmq_send_to_peer(source_node, MessageType::RemoteJobFinished, gpid_str + output);
                std::cout << "[WD] Remote job " << global_pid << " done → node " << source_node << std::endl;
            }
        }

        // ── NodeDead : broadcaster à tous les clients ───────
        // on reçoit l'id du nœud mort depuis le heartbeat thread
        // chaque client va filtrer dans sa job_table et relancer ce qui lui appartient
        if (items[2].revents & ZMQ_POLLIN) {
            zmq::message_t notif;
            pair_nodedead.recv(notif);
            std::string dead_node_str(static_cast<char*>(notif.data()), notif.size());
            for (const std::string& cid : connected_clients)
                zmq_send_to_client(router, cid, MessageType::NodeDead, dead_node_str);
            std::cout << "[ND] Node " << dead_node_str << " dead, notified "
                      << connected_clients.size() << " client(s)" << std::endl;
        }

        // ── Messages réseau ─────────────────────────────────
        if (!(items[0].revents & ZMQ_POLLIN)) continue;

        zmq::message_t sender_frame, type_frame, payload_frame;
        router.recv(sender_frame);
        router.recv(type_frame);
        router.recv(payload_frame);

        std::string sender_id(static_cast<char*>(sender_frame.data()), sender_frame.size());
        MessageType type    = static_cast<MessageType>(*static_cast<uint8_t*>(type_frame.data()));
        std::string message(static_cast<char*>(payload_frame.data()), payload_frame.size());

        std::cout << "Message from " << sender_id << " [" << static_cast<int>(type) << "]: " << message << std::endl;

        switch (type) {

        case MessageType::Heartbeat: {
            // mise à jour de la charge et du statut du pair
            // si le pair était Dead on ignore — un nœud mort reste mort
            int idx = (sender_id.substr(0, 10) == "heartbeat_")
                      ? std::stoi(sender_id.substr(10)) : std::stoi(sender_id);
            peers_mutex.lock();
            if (peers[idx].status == Liveness::Dead) {
                peers_mutex.unlock();
                std::cout << "Peer " << sender_id << " came back but ignoring.\n";
                break;
            }
            peers[idx].load           = std::stof(message);
            peers[idx].status         = Liveness::Alive;
            peers[idx].last_heartbeat = std::chrono::steady_clock::now();
            peers_mutex.unlock();
            break;
        }

        case MessageType::ClientCommand: {
            float my_load = std::stof(getLoadAverage());
            peers_mutex.lock();
            float global_charge = get_global_load(peers);
            int   target        = select_target(my_node_id, peers);
            peers_mutex.unlock();

            if (my_load > global_charge * 1.5f && target != my_node_id) {
                // jitter anti-honeypot : délai aléatoire avant de décider
                // évite que plusieurs nœuds surchargés envoient tous vers la même cible
                // et laisse le temps à /proc/stat de monter sur la cible
                std::mt19937 gen(std::random_device{}());
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(std::uniform_int_distribution<>(0, 300)(gen)));

                // recalcul après le délai — la situation a peut-être changé
                peers_mutex.lock();
                global_charge = get_global_load(peers);
                target        = select_target(my_node_id, peers);
                peers_mutex.unlock();

                if (target != my_node_id) {
                    bool bg = !message.empty() && message.back() == '&';
                    std::cout << "[LB] Redirecting to node " << target
                              << (bg ? " (bg)" : " (fg)") << "\n";
                    // on mémorise le client qui attend la réponse
                    if (bg) pending_bg_redirections.insert({target, sender_id});
                    else    pending_redirections[target] = sender_id;
                    zmq_send_to_peer(target, MessageType::RemoteCommand, message);
                    break;
                }
            }

            // exécution locale
            {
                bool     bg         = !message.empty() && message.back() == '&';
                uint32_t global_pid = fork_and_exec(message, bg, sender_id, false);
                // pour bg : on répond immédiatement avec le global_pid
                // pour fg : on attend que le watchdog notifie la fin
                if (bg) {
                    zmq_send_to_client(router, sender_id, MessageType::ClientReturnValue,
                                       "global_pid:" + std::to_string(global_pid));
                }
            }
            break;
        }

        case MessageType::RemoteCommand: {
            // commande reçue d'un autre nœud — on l'exécute localement
            // sender_id = node_id du nœud source (pas un routing_id client)
            bool     bg         = !message.empty() && message.back() == '&';
            uint32_t global_pid = fork_and_exec(message, bg, sender_id, true);
            if (bg) {
                // bg remote : on renvoie le global_pid au nœud source
                // lui le transmettra à son client
                zmq_send_to_peer(std::stoi(sender_id), MessageType::RemoteReturnValue,
                                 "global_pid:" + std::to_string(global_pid));
            }
            // fg remote : on attend RemoteJobFinished du watchdog, pas de réponse immédiate
            break;
        }

        case MessageType::RemoteReturnValue: {
            // reçu uniquement pour les jobs background distants
            // contient le global_pid → on le transmet au client qui attend
            int source_node = std::stoi(sender_id);
            auto it = pending_bg_redirections.find(source_node);
            if (it == pending_bg_redirections.end()) {
                std::cout << "[ERR] RemoteReturnValue from " << sender_id << " but no pending client\n";
                break;
            }
            zmq_send_to_client(router, it->second, MessageType::ClientReturnValue, message);
            // NB: on n'efface pas ici — on garde l'entrée jusqu'à RemoteJobFinished
            break;
        }

        case MessageType::RemoteJobFinished: {
            int source_node = std::stoi(sender_id);

            // foreground redirigé : le client attendait ce message pour débloquer
            auto it_fg = pending_redirections.find(source_node);
            if (it_fg != pending_redirections.end()) {
                zmq_send_to_client(router, it_fg->second, MessageType::ClientReturnValue, message);
                std::cout << "[WD] Remote FG done → client " << it_fg->second << std::endl;
                pending_redirections.erase(it_fg);
                break;
            }

            // background redirigé : notification asynchrone au client
            auto it = pending_bg_redirections.find(source_node);
            if (it == pending_bg_redirections.end()) {
                std::cout << "[ERR] RemoteJobFinished from " << sender_id << " but no pending client\n";
                break;
            }
            zmq_send_to_client(router, it->second, MessageType::JobFinished, message);
            std::cout << "[WD] Remote BG done → client " << it->second << std::endl;
            pending_bg_redirections.erase(it);
            break;
        }

        case MessageType::ClientHandshake: {
            // enregistrement du client — son routing_id devient son identité permanente
            connected_clients.insert(sender_id);
            zmq::message_t empty_payload;
            zmq::message_t cframe(sender_id.begin(), sender_id.end());
            uint8_t ack = static_cast<uint8_t>(MessageType::ClientAcknowledgement);
            zmq::message_t ack_type(sizeof(uint8_t));
            memcpy(ack_type.data(), &ack, sizeof(uint8_t));
            router.send(cframe,        zmq::send_flags::sndmore);
            router.send(ack_type,      zmq::send_flags::sndmore);
            router.send(empty_payload, zmq::send_flags::none);
            std::cout << "[HS] Client " << sender_id << " connected ("
                      << connected_clients.size() << " total)\n";
            break;
        }

        case MessageType::Stop: {
            // Ctrl+C côté client — il ne connaît pas le pid ni le nœud
            // c'est nous qui cherchons le job foreground de ce client
            bg_mutex.lock();
            for (auto& [pid, job] : background_jobs) {
                if (job.sender_id == sender_id && job.is_foreground) {
                    kill(pid, SIGINT);
                    std::cout << "[STOP] Killed fg job " << pid << " for " << sender_id << std::endl;
                    bg_mutex.unlock();
                    goto stop_done;
                }
            }
            bg_mutex.unlock();
            // job pas ici → c'est qu'on l'a redirigé vers un autre nœud
            for (auto& [target_node, client_id] : pending_redirections) {
                if (client_id == sender_id) {
                    zmq_send_to_peer(target_node, MessageType::Stop, "");
                    std::cout << "[STOP] Forwarded Stop to node " << target_node << std::endl;
                    break;
                }
            }
            stop_done:
            break;
        }

        case MessageType::RemoteKill: {
            // payload : "target_node:sig:local_pid"
            auto p1 = message.find(':'), p2 = message.find(':', p1+1);
            int   target_node = std::stoi(message.substr(0, p1));
            int   sig         = std::stoi(message.substr(p1+1, p2-p1-1));
            pid_t lpid        = std::stoi(message.substr(p2+1));
            if (target_node == my_node_id) {
                kill(lpid, sig);
            } else {
                // pas pour nous → on forward au bon nœud
                zmq_send_to_peer(target_node, MessageType::RemoteKill,
                    std::to_string(target_node) + ":" + std::to_string(sig) + ":" + std::to_string(lpid));
            }
            break;
        }

        default:
            std::cout << "Unknown message type: " << static_cast<int>(type) << std::endl;
            break;
        }
    }

    return 0;
}
