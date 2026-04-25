#pragma once

#include "cluster_state.hpp"

int select_target(int node_id, const std::vector<PeerInfo>& peers);
float get_global_load(const std::vector<PeerInfo>& peers);