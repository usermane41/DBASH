#include "load_balancer.hpp"

int select_target(int node_id, const std::vector<PeerInfo>& peers){
    int res=node_id;//fallback au cas ou meme si c'est le seuil qui gere 
    float load= std::numeric_limits<float>::max();
    for(int i=0;i<peers.size();i++){
        if(i!=node_id){
            if(peers[i].load<load && peers[i].status==Liveness::Alive){
                load=peers[i].load;
                res=i;
            }
        }
    }
    return res;
}

float get_global_load(const std::vector<PeerInfo>& peers){
    float res=0.0f;
    int vivant=0;
    for(int i=0;i<peers.size();i++){
        if(peers[i].status==Liveness::Alive){
            res+= peers[i].load;
            vivant+=1;
        }
    }
    if(!vivant){
        return 0;//si personne de dispo declancher localment par defaut
    }
    return res/vivant;
}