#include "message_types.hpp"

#include <zmq.hpp>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <id>\n";
        return 1;
    }

    std::string id_str = argv[1];
    int id = std::stoi(id_str);

    // ---- Load config ----
    const std::string config_path = "./config_servers.txt";
    std::ifstream file(config_path);

    if (!file) {
        std::cerr << "Failed to open config file\n";
        return 1;
    }

    std::vector<std::string> endpoints;
    std::string line;
    while (std::getline(file, line)) {
        if (!line.empty())
            endpoints.push_back(line);
    }

    if (id < 0 || id >= static_cast<int>(endpoints.size())) {
        std::cerr << "Invalid id index\n";
        return 1;
    }

    std::string endpoint = endpoints[id];

    // ---- ZMQ setup ----
    zmq::context_t ctx{1};
    zmq::socket_t dealer{ctx, zmq::socket_type::dealer};

    // Identity (must be set before connect)
    std::string client_identity = "client_" + id_str;
    dealer.set(zmq::sockopt::routing_id, client_identity);

    // Linger = 0
    dealer.set(zmq::sockopt::linger, 0);

    dealer.connect(endpoint);

    // ---- Initial send ----
    MessageType initial_send_type = MessageType::ClientHandshake;
    std::string payload = "";

    // Convert enum to byte
    uint8_t type_byte = static_cast<uint8_t>(initial_send_type);
    // zmq::message_t type_frame(&type_byte, sizeof(type_byte));  // 1-byte frame - Replaced with copy instead of pointer passing because it is safer
    zmq::message_t type_frame(sizeof(type_byte));
    memcpy(type_frame.data(), &type_byte, sizeof(type_byte));
    zmq::message_t payload_frame(payload.begin(), payload.end());

    // Send
    dealer.send(type_frame, zmq::send_flags::sndmore);
    dealer.send(payload_frame, zmq::send_flags::none);

    // ---- Initial receive (2 frames) ----
    {
        zmq::message_t type_frame;
        zmq::message_t payload_frame;

        dealer.recv(type_frame);
        dealer.recv(payload_frame);

        MessageType initial_receive_type = static_cast<MessageType>(*static_cast<uint8_t*>(type_frame.data()));
        std::string message(static_cast<char*>(payload_frame.data()), payload_frame.size());

        std::cout << "Received: [" << static_cast<int>(initial_receive_type) << "]: " << message << std::endl;
    }

    // ---- Interactive loop ----
    while (true) {
        std::cout << "> " << std::flush;
        std::string input;
        
        if (!std::getline(std::cin, input))
            break;

        if (input.empty())
            continue;

        // Send 2 frames
        MessageType send_type = MessageType::ClientCommand;

        // Convert enum to byte
        uint8_t type_byte = static_cast<uint8_t>(send_type);
        // zmq::message_t type_frame(&type_byte, sizeof(type_byte));  // 1-byte frame - Replaced with copy instead of pointer passing because it is safer
        zmq::message_t type_frame(sizeof(type_byte));
        memcpy(type_frame.data(), &type_byte, sizeof(type_byte));
        zmq::message_t payload_frame(input.begin(), input.end());

        // Send
        dealer.send(type_frame, zmq::send_flags::sndmore);
        dealer.send(payload_frame, zmq::send_flags::none);

        // Receive 2 frames
        zmq::message_t type_frame_response;
        zmq::message_t payload_frame_response;

        dealer.recv(type_frame_response);
        dealer.recv(payload_frame_response);

        MessageType receive_type = static_cast<MessageType>(*static_cast<uint8_t*>(type_frame_response.data()));
        std::string message(static_cast<char*>(payload_frame_response.data()), payload_frame_response.size());

        std::cout << "Received: [" << static_cast<int>(receive_type) << "]: " << message << std::endl;
    }

    return 0;
}