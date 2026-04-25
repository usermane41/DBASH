#include <iostream>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <sys/wait.h>
#include "repl.hpp"
#include "parser.hpp"
#include "job_table.hpp"
#include "load_balancer.hpp"

static constexpr const char* PROMPT = "dbash> ";

/* ---------- built-ins (stubs pour l'instant) ---------- */

[[noreturn]] static void builtin_exit()
{
    std::cout << "Bye.\n";
    exit(0);
}

static void builtin_ps()
{
    std::lock_guard<std::mutex> lock(job_mutex);
    std::cout << "global_pid   node   command   status\n";
    for(Job& j : job_table){//on s'evite de copier
        std::cout << j.global_pid << "   "<< j.node_id <<"   " <<j.command << "   "<<j.finished << "\n";
    }
}

static void builtin_wait_all()
{
   bool finished =false;
    while(!finished){
        finished=true;
        {
            std::lock_guard<std::mutex> lock(job_mutex);
            for(Job& j: job_table){
            if(!j.finished && j.background){//on n'attend que se lancer avec &
                finished=false;
                }
            }
        }
        if(!finished) sleep(1); 
    }
}

static void builtin_kill(const Command& cmd)
{
    int sig = std::stoi(cmd.args[1].substr(1));//converti en int et enleve le - par la meme occasion
    uint32_t gpid = std::stoul(cmd.args[2]);

    Job *j = find_job(gpid);
    if (j == nullptr) {
        std::cout << "kill: pid " << gpid << " introuvable\n";
        return;
    }
    pid_t local_pid;
    local_pid=j->local_pid;
    kill(local_pid,sig);

}

/* ---------- dispatch ---------- */

static bool handle_builtin(const Command& cmd)
{
    const auto& name = cmd.name();
    if (name == "exit") { builtin_exit(); }
    if (name == "ps")   { builtin_ps();        return true; }
    if (name == "wait") { builtin_wait_all();  return true; }
    if (name == "kill") { builtin_kill(cmd);   return true; }
    return false;
}

static void execute_command(const Command& cmd)
{
    if (handle_builtin(cmd)) return;

    /*
     * TODO étape 2 : interroger le load-balancer pour choisir la machine cible,
     *               puis fork/exec local ou envoi réseau.
     */
    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        return;
    }
    if (pid == 0) {
        // Enfant — on construit argv[] juste avant execvp
        auto argv = cmd.argv();
        execvp(argv[0], argv.data());
        perror(argv[0]);
        _exit(127);
    }
    // Parent
    if (!cmd.background) {
        int status;
        waitpid(pid, &status, 0);
    } else {
        Job j;
        j.local_pid  = pid;
        j.global_pid = (0 << 16) | pid;  // node_id=0 pour l'instant
        j.node_id    = 0;
        j.command    = cmd.name();
        j.background = cmd.background;
        j.finished   = false;
        add_job(j);
    }
}

/* ---------- boucle principale ---------- */

void repl_run()
{
    char* raw;
    while ((raw = readline(PROMPT)) != nullptr) {
        std::string line(raw);
        if (!line.empty()) add_history(raw);
        free(raw);

        Command cmd = parse_line(line);
        if (cmd.empty()) continue;
        execute_command(cmd);
        if (sigchld_received.exchange(false)) {
            pid_t pid;
            int status;
            while ((pid = waitpid(-1, &status, WNOHANG)) > 0)
                mark_finished(pid);
        }
    }

    // EOF (Ctrl+D)
    std::cout << "\n";
    builtin_exit();
}
