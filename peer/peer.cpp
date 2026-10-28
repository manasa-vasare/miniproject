// ============================================================================
// peer.cpp - Peer process (spec §5, §10)
//
// A peer is an independent OS process running a TCP server. It:
//   - answers REGISTER        (handshake with the controller)
//   - answers HEARTBEAT       (E4 failure detection probes)
//   - accepts STORE_CHUNK     (E3 distribution/replication writes)
//   - serves  GET_CHUNK       (E5 recovery reads) from its OWN storage only
//
// Each peer has isolated storage: storage/peer_<ID>/chunks/.
// A peer can only serve chunks that actually exist in its own storage.
//
// Usage: peer <ID> <address> <port>
// ============================================================================
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "../engine/chunking/chunk_manager.hpp"
#include "../engine/communication/protocol.hpp"
#include "../engine/communication/tcp_server.hpp"
#include "../engine/process/process_manager.hpp"

namespace fs = std::filesystem;

namespace {
// Chunk ids come from E1 as Fnnn-Cnnn, but never trust the wire: reject
// anything that could escape the storage directory on POSIX or Windows
// (separators, "..", drive/stream colons, control characters).
bool safe_chunk_id(const std::string& id) {
    if (id.empty() || id.size() > 128 || id.find("..") != std::string::npos) return false;
    for (unsigned char c : id) {
        if (c < 0x20 || c == '/' || c == '\\' || c == ':' || c == '*' || c == '?' ||
            c == '"' || c == '<' || c == '>' || c == '|')
            return false;
    }
    return true;
}

std::vector<uint8_t> read_binary(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return {};
    return std::vector<uint8_t>(std::istreambuf_iterator<char>(in),
                                std::istreambuf_iterator<char>());
}
}  // namespace

int main(int argc, char** argv) {
    if (argc != 4) {
        std::cerr << "usage: peer <ID> <address> <port>\n";
        return 1;
    }
    const std::string id = argv[1];
    const std::string address = argv[2];
    int port_i = 0;
    try {
        port_i = std::stoi(argv[3]);
    } catch (...) {
        port_i = 0;
    }
    if (port_i < 1 || port_i > 65535) {
        std::cerr << "invalid port: " << argv[3] << "\n";
        return 1;
    }
    const auto port = static_cast<uint16_t>(port_i);

    if (!sock_init()) {
        std::cerr << "[peer " << id << "] socket subsystem initialisation failed\n";
        return 1;
    }

    // Isolated per-peer storage (spec §10).
    const std::string storage_dir = "storage/peer_" + id + "/chunks";
    fs::create_directories(storage_dir);

    TCPServer::Handler handler =
        [&id, &storage_dir](const json::Json& req,
                            const std::vector<uint8_t>& req_payload,
                            json::Json& resp,
                            std::vector<uint8_t>& resp_payload) -> bool {
        const std::string type = req["type"].as_string();

        if (type == proto::REGISTER) {
            resp["type"] = proto::REGISTER_ACK;
            resp["peer_id"] = id;
            resp["status"] = "OK";
            return true;
        }

        if (type == proto::HEARTBEAT) {
            resp["type"] = proto::HEARTBEAT_ACK;
            resp["peer_id"] = id;
            resp["status"] = "ALIVE";
            return true;
        }

        if (type == proto::STORE_CHUNK) {
            const std::string chunk_id = req["chunk_id"].as_string();
            // Guard against path traversal; chunk ids come from E1 as
            // Fnnn-Cnnn but never trust the wire.
            if (!safe_chunk_id(chunk_id)) {
                resp["type"] = proto::TRANSFER_ERROR;
                resp["reason"] = "bad chunk_id";
                return true;
            }
            // If the sender supplied the expected digest, verify integrity
            // before accepting the chunk (never accept corrupt data).
            if (req.contains("sha256")) {
                const std::string got =
                    ChunkManager::sha256_of_bytes(req_payload);
                if (got != req["sha256"].as_string()) {
                    resp["type"] = proto::TRANSFER_ERROR;
                    resp["reason"] = "sha256 mismatch on store";
                    return true;
                }
            }
            const std::string path = storage_dir + "/" + chunk_id + ".chunk";
            const std::string tmp = path + ".tmp";
            {
                std::ofstream out(tmp, std::ios::binary);
                out.write(reinterpret_cast<const char*>(req_payload.data()),
                          static_cast<std::streamsize>(req_payload.size()));
            }
            fs::rename(tmp, path);  // atomic-ish publish; no partial chunks
            resp["type"] = proto::STORE_ACK;
            resp["chunk_id"] = chunk_id;
            resp["status"] = "OK";
            resp["size"] = static_cast<long long>(req_payload.size());
            return true;
        }

        if (type == proto::GET_CHUNK) {
            const std::string chunk_id = req["chunk_id"].as_string();
            if (!safe_chunk_id(chunk_id)) {
                resp["type"] = proto::TRANSFER_ERROR;
                resp["reason"] = "bad chunk_id";
                return true;
            }
            const std::string path = storage_dir + "/" + chunk_id + ".chunk";
            if (!fs::exists(path)) {
                // Only chunks that really exist in own storage are served.
                resp["type"] = proto::TRANSFER_ERROR;
                resp["reason"] = "chunk not found on peer " + id;
                return true;
            }
            std::vector<uint8_t> data = read_binary(path);
            resp["type"] = proto::CHUNK_RESPONSE;
            resp["chunk_id"] = chunk_id;
            resp["peer_id"] = id;

            // Test hook (T03): simulate an interrupted transfer. The server
            // sends only half the payload, then closes the connection; the
            // receiver must detect the short read, discard the temporary
            // file and retry the complete chunk transfer (spec §8.12).
            if (req.contains("simulate") &&
                req["simulate"].as_string() == "interrupt") {
                resp["truncate_payload_to"] =
                    static_cast<long long>(data.size() / 2);
            }
            resp_payload = std::move(data);
            return true;
        }

        resp["type"] = proto::TRANSFER_ERROR;
        resp["reason"] = "unknown message type: " + type;
        return true;
    };

    TCPServer server(address, port, handler);
    if (!server.start()) {
        std::cerr << "[peer " << id << "] failed to bind " << address << ":"
                  << port << "\n";
        return 1;
    }
    std::cout << "[peer " << id << "] ONLINE at " << address << ":" << port
              << " storage=" << storage_dir << std::endl;

    // Controller-requested shutdown (SIGINT/SIGTERM on POSIX, the named stop
    // event or a console control event on Windows). The callback runs on a
    // helper thread, never in signal context.
    proc::install_shutdown_handler([&server]() { server.stop(); });
    server.run();
    proc::remove_shutdown_handler();
    std::cout << "[peer " << id << "] stopped" << std::endl;
    return 0;
}
