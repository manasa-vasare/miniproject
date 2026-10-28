// ============================================================================
// chunk.hpp - Core data model: Chunk metadata (E1/E6)
//
// chunk_id = identity, sequence = original position, SHA-256 = integrity.
// ============================================================================
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "../third_party/mini_json.hpp"

struct ChunkMeta {
    std::string chunk_id;              // e.g. "F001-C001"
    std::string file_id;               // e.g. "F001"
    std::string filename;              // original file name
    uint64_t    sequence = 0;          // 1-based original position
    uint64_t    size = 0;              // bytes in this chunk
    std::string sha256;                // hex digest of the chunk payload
    std::vector<std::string> peers;    // replica locations, e.g. ["A","B"]

    json::Json to_json() const {
        json::Json j = json::Json::object();
        j["sequence"] = static_cast<long long>(sequence);
        j["size"]     = static_cast<long long>(size);
        j["sha256"]   = sha256;
        json::Json locs = json::Json::array();
        for (const auto& p : peers) locs.push_back(p);
        j["peers"] = locs;
        return j;
    }

    static ChunkMeta from_json(const std::string& chunk_id,
                               const std::string& file_id,
                               const std::string& filename,
                               const json::Json& j) {
        ChunkMeta c;
        c.chunk_id = chunk_id;
        c.file_id  = file_id;
        c.filename = filename;
        c.sequence = static_cast<uint64_t>(j["sequence"].as_int());
        c.size     = static_cast<uint64_t>(j["size"].as_int());
        c.sha256   = j["sha256"].as_string();
        for (const auto& p : j["peers"].items()) c.peers.push_back(p.as_string());
        return c;
    }
};
