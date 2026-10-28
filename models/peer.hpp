// ============================================================================
// peer.hpp - Core data model: Peer (E2/E4)
// ============================================================================
#pragma once

#include <cstdint>
#include <string>

enum class PeerState { ONLINE, OFFLINE, UNKNOWN };

inline const char* peer_state_str(PeerState s) {
    switch (s) {
        case PeerState::ONLINE:  return "ONLINE";
        case PeerState::OFFLINE: return "OFFLINE";
        default:                 return "UNKNOWN";
    }
}

struct PeerInfo {
    std::string id;        // "A", "B", "C"
    std::string address;   // "127.0.0.1"
    uint16_t    port = 0;  // 5001, 5002, 5003
    PeerState   state = PeerState::UNKNOWN;

    std::string storage_dir() const { return "storage/peer_" + id + "/chunks"; }
};
