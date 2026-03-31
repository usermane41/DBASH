#include "message_types.hpp"

#include <zmq.hpp>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <sstream>
#include <chrono>

struct PeerInfo {
    float load = 0.0f;  // current load
    std::chrono::steady_clock::time_point last_heartbeat; // timestamp
};

std::string getLoadAverage() {
    std::ifstream load_file("/proc/loadavg");
    if (!load_file.is_open()) return "error reading loadavg";

    std::string line;
    std::getline(load_file, line);  // e.g., "0.12 0.34 0.56 1/100 12345"
    
    std::istringstream iss(line);
    std::string firstNumber;
    iss >> firstNumber;  // extract the first number

    return firstNumber;
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <node_id>" << std::endl;
        return 1;
    }

    int node_id = std::stoi(argv[1]);

    // Hardcoded config path
    const std::string config_path = "./config_servers.txt";

    // Read config file
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

    int num_nodes = addresses.size();

    if (node_id < 0 || node_id >= num_nodes) {
        std::cerr << "Invalid node_id\n";
        return 1;
    }

    std::cout << "Total nodes: " << num_nodes << std::endl;

    // Create ZeroMQ context and ROUTER socket
    zmq::context_t context(1);
    zmq::socket_t router(context, zmq::socket_type::router);

    // Set identity (as string)
    std::string identity = std::to_string(node_id);
    router.set(zmq::sockopt::routing_id, identity);

    // Prevents the socket from remaining open after quitting forcefully
    router.set(zmq::sockopt::linger, 0);

    // Bind to this node's address
    router.bind(addresses[node_id]);
    std::cout << "Node " << node_id << " bound to " << addresses[node_id] << std::endl;

    // Create a vector of DEALER sockets for all other nodes
    std::vector<PeerInfo> peers(num_nodes);
    std::vector<zmq::socket_t> dealers(num_nodes);

    for (int j = 0; j < num_nodes; ++j) {
        if (j == node_id) continue;

        // Initialize DEALER socket for peer j
        dealers[j] = zmq::socket_t(context, zmq::socket_type::dealer);

        // Set the DEALER identity to our node's ID
        dealers[j].set(zmq::sockopt::routing_id, identity);

        // Prevents the socket from remaining open after quitting forcefully
        dealers[j].set(zmq::sockopt::linger, 0);

        // Connect to the peer's address
        dealers[j].connect(addresses[j]);

        // Send initial "hello world" to the peer
        MessageType type = MessageType::Heartbeat;
        std::string payload = getLoadAverage();

        // Convert enum to byte
        uint8_t type_byte = static_cast<uint8_t>(type);
        zmq::message_t type_frame(&type_byte, sizeof(type_byte));  // 1-byte frame
        zmq::message_t payload_frame(payload.begin(), payload.end());

        // Send
        dealers[j].send(type_frame, zmq::send_flags::sndmore);
        dealers[j].send(payload_frame, zmq::send_flags::none);

        std::cout << "Connected DEALER to node " << j << " and sent load : " << payload << std::endl;
    }

    // Receive loop
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

        std::cout << "Message received from " << sender_id << " [" << static_cast<int>(type) << "]: " << message << std::endl;

        switch(type) {
            case MessageType::Heartbeat: {
                break;
            }
            
            case MessageType::ClientCommand: {
                response_type = MessageType::ClientReturnValue;
                response_payload = "Command received: " + message;

                uint8_t resp_type_byte = static_cast<uint8_t>(response_type);
                zmq::message_t resp_type_frame(&resp_type_byte, sizeof(resp_type_byte));  // 1-byte frame
                zmq::message_t resp_payload_frame(response_payload.begin(), response_payload.end());

                router.send(sender, zmq::send_flags::sndmore);
                router.send(resp_type_frame, zmq::send_flags::sndmore);
                router.send(resp_payload_frame, zmq::send_flags::none);

                break;
            }

            case MessageType::ClientHandshake: {
                response_type = MessageType::ClientAcknowledgement;
                response_payload = "";

                uint8_t resp_type_byte = static_cast<uint8_t>(response_type);
                zmq::message_t resp_type_frame(&resp_type_byte, sizeof(resp_type_byte));  // 1-byte frame
                zmq::message_t resp_payload_frame(response_payload.begin(), response_payload.end());

                router.send(sender, zmq::send_flags::sndmore);
                router.send(resp_type_frame, zmq::send_flags::sndmore);
                router.send(resp_payload_frame, zmq::send_flags::none);

                break;
            }

            default: {
                std::cout << "Unknown message type, this indicates an error with the protocol. You should probably switch to E-Bash anyway." << message << std::endl;
                break;
            }
        }


    }

    return 0;
}