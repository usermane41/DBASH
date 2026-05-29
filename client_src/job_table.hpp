#pragma once
#include <vector>
#include <mutex>
#include <string>
#include <cstdint>
#include <sys/types.h>
#include <sys/wait.h>
#include <atomic>
extern std::atomic<bool> sigchld_received;

struct Job{
    uint32_t global_pid; //le pid unique sur le reseau
    pid_t local_pid;
    int node_id;//le numero de machine
    std::string command;//savoir quelle cmd a été lancer
    bool background;//si lancer avec &
    bool finished;//savoir si le job est fini ou pas
    bool failed = false;//savoir si le job a echouer ou pas
};

extern std::vector<Job> job_table;//la liste de tout les job
extern std::mutex job_mutex; //mutex qui delay les signaux lorsqu'on push back

void add_job(Job j);
Job* find_job(uint32_t j);//servira pour le kill
void mark_finished(pid_t local_pid);//pour le wait
