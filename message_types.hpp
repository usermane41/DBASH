#pragma once

#include <cstdint>

enum class MessageType : uint8_t {
    Heartbeat             = 1,
    ClientHandshake       = 2,
    ClientAcknowledgement = 3,
    ClientCommand         = 4,
    ClientReturnValue     = 5,
    RemoteCommand         = 6,
    RemoteReturnValue     = 7,
    RemoteKill            = 8,
    JobFinished           = 9,
    RemoteJobFinished     = 10,
    NodeDead              = 11,
    Stop                  = 13
};