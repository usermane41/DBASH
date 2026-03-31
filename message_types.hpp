#pragma once   // include guard

#include <cstdint>

enum class MessageType : uint8_t {
    Heartbeat = 1,
    ClientHandshake = 2,
    ClientAcknowledgement = 3,
    ClientCommand = 4,
    ClientReturnValue = 5
};