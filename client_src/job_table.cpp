#include "job_table.hpp"
std::vector<Job> job_table;
std::mutex job_mutex;

void add_job(Job j){
    std::lock_guard<std::mutex> lock(job_mutex);
    job_table.push_back(j);
}

Job* find_job(uint32_t global_pidj){
    std::lock_guard<std::mutex> lock(job_mutex);
    for(Job &j: job_table){//&on s'assure de modifier l'original
        if(j.global_pid == global_pidj){
            return &j;
        }
    }
    return NULL;
}

void mark_finished(pid_t local_pid){
    std::lock_guard<std::mutex> lock(job_mutex);
    for(Job &j: job_table){
        if(j.local_pid == local_pid){
            j.finished=true;
        }
    }
}

