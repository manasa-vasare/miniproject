// ============================================================================
// controller.cpp - Controller: orchestrates the six engines (spec §14)
//
// The Controller composes E1..E6 without absorbing their responsibilities:
//   E1 ChunkManager     - chunking, SHA-256, chunk metadata
//   E2 TCPClient        - request/response communication with peers
//   E3 Distributor      - round-robin placement + replication
//   E4 FailureDetector  - heartbeat-based failure detection
//   E5 RecoveryManager  - recovery + reconstruction
//   E6 MetadataManager  - metadata + monitoring
//
// CLI:
//   controller sys <start|stop|status>   start / stop / list the 3 peer processes
//   controller fail <A|B|C|random>       terminate one peer (simulated failure)
//   controller demo [file]               full Gate-1 demonstration
//   controller test [T01 ...]            run the Gate-1 tests T01..T07
//   controller chunk <file>
//   controller distribute
//   controller ping
//   controller detect [--all] [--duration-ms N]
//   controller recover [--failed <id>] [--simulate-interrupt]
//   controller reconstruct [original_file]
//   controller status
//   controller reset
//
// The controller finds the project root (the directory holding config.json)
// by itself, so it can be started from any working directory.
// ============================================================================
#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <iterator>
#include <mutex>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <random>
#include <string>
#include <sstream>
#include <thread>
#include <vector>

#include "../engine/chunking/chunk_manager.hpp"
#include "../engine/communication/socket_compat.hpp"
#include "../engine/communication/tcp_client.hpp"
#include "../engine/distribution/distributor.hpp"
#include "../engine/failure_detection/failure_detector.hpp"
#include "../engine/metadata/metadata_manager.hpp"
#include "../engine/recovery/recovery_manager.hpp"
#include "../engine/process/process_manager.hpp"
#include "../models/peer.hpp"
#include "../third_party/mini_json.hpp"

namespace fs = std::filesystem;

class Controller {
public:
    Controller() { load_config(); }

    // ---- configuration -------------------------------------------------------
    void load_config() {
        std::ifstream in("config.json");
        if (!in) throw std::runtime_error("config.json not found - run from the project root");
        std::stringstream ss;
        ss << in.rdbuf();
        config_ = json::Json::parse(ss.str());

        chunk_size_ = static_cast<uint64_t>(config_["chunk_size"].as_int());
        hb_interval_ = static_cast<int>(config_["heartbeat_interval_ms"].as_int());
        hb_timeout_ = static_cast<int>(config_["heartbeat_timeout_ms"].as_int());
        hb_threshold_ = static_cast<int>(config_["failure_threshold"].as_int());

        for (const auto& p : config_["peers"].items()) {
            PeerInfo pi;
            pi.id = p["id"].as_string();
            pi.address = p["address"].as_string();
            pi.port = static_cast<uint16_t>(p["port"].as_int());
            peers_.push_back(pi);
        }
    }

    // ---- E1: chunk ------------------------------------------------------------
    int cmd_chunk(const std::string& file) {
        std::error_code fec;
        const fs::path file_path = fs::u8path(file);  // UTF-8 -> native path
        if (!fs::is_regular_file(file_path, fec)) {
            std::cerr << "input file not found: " << file << "\n";
            return 1;
        }
        const uint64_t size = fs::file_size(file_path);
        const std::string fid = meta_.next_file_id();

        ChunkManager cm(chunk_size_);
        auto chunks = cm.chunk_file(fid, file, staging_dir());

        meta_.init_file_metadata(fid, file_path.filename().u8string(),
                                 size, chunk_size_, chunks);
        // Remember where the original lives, for the final byte-comparison.
        {
            json::Json d = json::Json::object();
            d["file_id"] = fid;
            d["source_path"] = fs::absolute(file_path).u8string();
            meta_.log_event("FILE_CHUNKED", "file split into fixed-size chunks", d);
        }
        // Store source path in metadata for reconstruct's default.
        {
            // cheap approach: separate sidecar, keeps file_metadata.json schema clean
            std::ofstream out("metadata/source_path.txt", std::ios::trunc);
            out << fs::absolute(file_path).u8string();
        }

        std::cout << "[E1] file=" << file << " id=" << fid << " size=" << size
                  << " chunks=" << chunks.size() << " chunk_size=" << chunk_size_
                  << "\n";
        for (const auto& c : chunks)
            std::cout << "  " << c.chunk_id << " seq=" << c.sequence
                      << " size=" << c.size << " sha256=" << c.sha256.substr(0, 16)
                      << "...\n";
        return 0;
    }

    // ---- E3: distribute ---------------------------------------------------------
    int cmd_distribute() {
        if (!meta_.has_file_metadata()) {
            std::cerr << "no file metadata - run 'chunk' first\n";
            return 1;
        }
        auto chunks = meta_.chunks();
        Distributor dist(peers_);
        std::string err;
        if (!dist.distribute(staging_dir(), chunks, err)) {
            std::cerr << "[E3] distribution failed: " << err << "\n";
            meta_.log_event("DISTRIBUTION_FAILED", err);
            return 1;
        }
        for (const auto& c : chunks) meta_.set_chunk_peers(c.chunk_id, c.peers);

        std::cout << "[E3] round-robin placement, replication factor 2:\n";
        for (const auto& c : chunks)
            std::cout << "  " << c.chunk_id << " -> " << c.peers[0] << " + "
                      << c.peers[1] << "\n";
        json::Json d = json::Json::object();
        d["total_chunks"] = static_cast<long long>(chunks.size());
        d["replication_factor"] = 2;
        meta_.log_event("DISTRIBUTION_DONE",
                        "chunks distributed round-robin with replication", d);
        return 0;
    }

    // ---- E2: REGISTER handshake ----------------------------------------------------
    bool ping_peer(const PeerInfo& p, int timeout_ms = 1000) {
        TCPClient client(timeout_ms, timeout_ms);
        if (!client.connect_to(p.address, p.port)) return false;
        json::Json req = json::Json::object();
        req["type"] = proto::REGISTER;
        json::Json resp;
        std::vector<uint8_t> pl;
        return client.request(req, {}, resp, pl) == proto::RecvStatus::OK &&
               resp["type"].as_string() == proto::REGISTER_ACK;
    }

    int cmd_ping() {
        bool all_ok = true;
        for (const auto& p : peers_) {
            const bool ok = ping_peer(p);
            std::cout << "  peer " << p.id << " (" << p.address << ":" << p.port
                      << ") " << (ok ? "ONLINE" : "UNREACHABLE") << "\n";
            all_ok &= ok;
        }
        return all_ok ? 0 : 1;
    }

    // ---- E4: heartbeat failure detection --------------------------------------------
    // duration_ms > 0 bounds the monitoring time (used with --all).
    int cmd_detect(bool stop_on_first, long long duration_ms = 0) {
        FailureDetector fd(peers_, hb_interval_, hb_timeout_, hb_threshold_);
        std::cout << "[E4] heartbeat monitor: interval=" << hb_interval_
                  << "ms timeout=" << hb_timeout_
                  << "ms threshold=" << hb_threshold_ << " missed\n";

        for (const auto& p : fd.probe_once())
            meta_.set_peer_status(p.id, p.state);

        std::mutex mu;
        std::string failed;
        long long detection_ms = 0;
        std::atomic<bool> finished{false};
        std::thread timer;
        if (duration_ms > 0) {
            timer = std::thread([&]() {
                const auto until = std::chrono::steady_clock::now() +
                                   std::chrono::milliseconds(duration_ms);
                while (!finished && std::chrono::steady_clock::now() < until)
                    std::this_thread::sleep_for(std::chrono::milliseconds(20));
                fd.stop();
            });
        }
        fd.run([&](const PeerInfo& peer, long long ms) {
            {
                std::lock_guard<std::mutex> lk(mu);
                if (failed.empty()) {
                    failed = peer.id;
                    detection_ms = ms;
                }
            }
            meta_.set_peer_status(peer.id, PeerState::OFFLINE);
            json::Json d = json::Json::object();
            d["peer"] = peer.id;
            d["detection_ms"] = ms;
            meta_.log_event("PEER_FAILURE_DETECTED",
                            "heartbeat threshold reached; peer marked OFFLINE", d);
            std::lock_guard<std::mutex> lk(mu);
            std::cout << "[E4] peer " << peer.id << " OFFLINE (detection time "
                      << ms << " ms, measured)\n";
        }, stop_on_first);
        finished = true;
        if (timer.joinable()) timer.join();

        if (!failed.empty()) {
            std::cout << failed << " " << detection_ms << "\n";  // machine-readable
            return 0;
        }
        return 2;  // no failure observed
    }

    // ---- failure simulation: terminate one peer process ------------------------------
    // Accepts a configured peer id (case-insensitive); returns the canonical id.
    bool resolve_peer_id(const std::string& wanted, std::string& id_out) const {
        for (const auto& p : peers_) {
            if (p.id.size() == wanted.size() &&
                std::equal(p.id.begin(), p.id.end(), wanted.begin(),
                           [](char x, char y) {
                               return std::toupper(static_cast<unsigned char>(x)) ==
                                      std::toupper(static_cast<unsigned char>(y));
                           })) {
                id_out = p.id;
                return true;
            }
        }
        return false;
    }

    bool kill_peer_process(const std::string& target, long& pid_out) {
        const long pid = proc::read_pid_file(proc::pid_file_for(target));
        if (pid <= 0) {
            std::cerr << "no pid file for peer " << target
                      << " (is the system running? try: controller sys start)\n";
            return false;
        }
        if (!proc::peer_running(pid)) {
            std::cerr << "peer " << target << " (pid " << pid
                      << ") is not running\n";
            return false;
        }
        std::string err;
        if (!proc::terminate_peer(pid, err)) {
            std::cerr << "failed to terminate peer " << target << ": " << err << "\n";
            return false;
        }
        pid_out = pid;
        return true;
    }

    int cmd_fail(const std::string& which) {
        std::string target;
        if (which == "random") {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<size_t> pick(0, peers_.size() - 1);
            target = peers_[pick(gen)].id;
        } else if (!resolve_peer_id(which, target)) {
            std::cerr << "unknown peer '" << which << "' (expected";
            for (const auto& p : peers_) std::cerr << " " << p.id;
            std::cerr << " or random)\n";
            return 1;
        }
        long pid = 0;
        if (!kill_peer_process(target, pid)) return 1;
        json::Json d = json::Json::object();
        d["peer"] = target;
        d["pid"] = pid;
        meta_.log_event("PEER_KILLED", "peer process terminated (simulated failure)", d);
        std::cout << "[demo] peer " << target << " (pid " << pid
                  << ") terminated - failure simulated\n";
        std::cout << target << "\n";
        return 0;
    }

    // ---- system process control (native; replaces the run_system script) ------------
    // Starts every peer that is not already running, then waits until all of
    // them answer the REGISTER handshake.
    int cmd_sys_start() {
        const std::string exe = proc::find_peer_exe();
        if (exe.empty()) {
            std::cerr << "peer executable not found next to the controller - build first:\n"
                      << "  cmake -S . -B build && cmake --build build --config Release\n";
            return 1;
        }
        std::error_code ec;
        fs::create_directories("run", ec);

        int rc = 0;
        std::vector<const PeerInfo*> waiting;
        for (const auto& p : peers_) {
            const long old_pid = proc::read_pid_file(proc::pid_file_for(p.id));
            if (old_pid > 0 && proc::peer_running(old_pid)) {
                std::cout << "peer " << p.id << " already running (pid " << old_pid << ")\n";
                waiting.push_back(&p);
                continue;
            }
            long pid = 0;
            std::string err;
            if (!proc::start_peer(exe, p.id, p.address, p.port, pid, err)) {
                std::cerr << "peer " << p.id << " failed to start: " << err << "\n";
                rc = 1;
                continue;
            }
            std::cout << "peer " << p.id << " started on " << p.address << ":"
                      << p.port << " (pid " << pid << ")\n";
            waiting.push_back(&p);
        }

        // Readiness: process alive AND REGISTER_ACK received (max ~10 s).
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!waiting.empty() && std::chrono::steady_clock::now() < deadline) {
            for (auto it = waiting.begin(); it != waiting.end();) {
                const long pid = proc::read_pid_file(proc::pid_file_for((*it)->id));
                if (!proc::peer_running(pid)) {
                    std::cerr << "peer " << (*it)->id << " exited during start-up - see "
                              << proc::log_file_for((*it)->id) << "\n";
                    print_log_tail((*it)->id);
                    rc = 1;
                    it = waiting.erase(it);
                } else if (ping_peer(**it, 300)) {
                    it = waiting.erase(it);
                } else {
                    ++it;
                }
            }
            if (!waiting.empty())
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        for (const PeerInfo* p : waiting) {
            std::cerr << "peer " << p->id << " did not become ready in time - see "
                      << proc::log_file_for(p->id) << "\n";
            print_log_tail(p->id);
            rc = 1;
        }
        if (rc == 0) std::cout << "all peers started and answering\n";
        return rc;
    }

    void print_log_tail(const std::string& id) {
        std::ifstream in(proc::log_file_for(id));
        std::string line;
        int n = 0;
        while (std::getline(in, line) && n < 20) { std::cerr << "    | " << line << "\n"; ++n; }
    }

    int cmd_sys_stop() {
        int rc = 0;
        for (const auto& p : peers_) {
            const std::string pidfile = proc::pid_file_for(p.id);
            const long pid = proc::read_pid_file(pidfile);
            if (pid <= 0) {
                std::cout << "peer " << p.id << " not running (no pid file)\n";
            } else if (!proc::peer_running(pid)) {
                std::cout << "peer " << p.id << " (pid " << pid
                          << ") not running (stale pid file removed)\n";
            } else {
                std::string err;
                if (proc::stop_peer(pid, err)) {
                    std::cout << "peer " << p.id << " (pid " << pid << ") stopped\n";
                } else {
                    std::cerr << "peer " << p.id << ": " << err << "\n";
                    rc = 1;
                    continue;  // keep the pid file: it may still be running
                }
            }
            proc::remove_pid_file(pidfile);
        }
        return rc;
    }

    // Returns 0 only if all peers are RUNNING.
    int cmd_sys_status() {
        int rc = 0;
        for (const auto& p : peers_) {
            const long pid = proc::read_pid_file(proc::pid_file_for(p.id));
            const bool alive = pid > 0 && proc::peer_running(pid);
            const char* state = "STOPPED";
            if (alive) state = ping_peer(p, 500) ? "RUNNING" : "UNRESPONSIVE";
            std::cout << "  peer " << p.id << " " << state;
            if (alive) std::cout << " (pid " << pid << ") " << p.address << ":" << p.port;
            std::cout << "\n";
            if (std::string(state) != "RUNNING") rc = 1;
        }
        return rc;
    }

    // ---- E5: recovery -------------------------------------------------------------------
    int cmd_recover(const std::string& failed_arg, bool simulate_interrupt) {
        if (!meta_.has_file_metadata()) {
            std::cerr << "no file metadata\n";
            return 1;
        }
        // Determine which peers are offline by probing (heartbeats, not TCP
        // liveness assumptions).
        FailureDetector fd(peers_, hb_interval_, hb_timeout_, hb_threshold_);
        std::vector<std::string> offline;
        for (const auto& p : fd.probe_once()) {
            meta_.set_peer_status(p.id, p.state);
            if (p.state == PeerState::OFFLINE) offline.push_back(p.id);
        }
        std::string failed = failed_arg;
        if (failed.empty()) {
            if (offline.empty()) {
                std::cerr << "no failed peer detected\n";
                return 1;
            }
            failed = offline.front();
        }

        RecoveryManager rm(peers_, meta_, output_dir());
        RecoveryResult r = rm.recover(failed, offline, simulate_interrupt);

        std::cout << "[E5] failed peer: " << failed << "\n";
        std::cout << "  affected chunks (" << r.affected_chunks.size() << "):";
        for (const auto& a : r.affected_chunks) std::cout << " " << a;
        std::cout << "\n  recovered (" << r.recovered_chunks.size() << "):";
        for (const auto& c : r.recovered_chunks) std::cout << " " << c;
        std::cout << "\n  retries=" << r.retry_count
                  << " integrity_failures=" << r.integrity_failures
                  << " bytes_transferred=" << r.bytes_transferred << "\n";
        if (!r.unrecoverable_chunks.empty()) {
            std::cout << "  UNRECOVERABLE:";
            for (const auto& u : r.unrecoverable_chunks) std::cout << " " << u;
            std::cout << "\n";
        }
        return r.success ? 0 : 3;
    }

    // ---- E5: reconstruction + final comparison ---------------------------------------------
    int cmd_reconstruct(const std::string& original) {
        std::string src = original;
        if (src.empty()) {
            std::ifstream in("metadata/source_path.txt");
            if (!in || !std::getline(in, src)) {
                std::cerr << "original file path unknown - pass it explicitly\n";
                return 1;
            }
        }
        RecoveryManager rm(peers_, meta_, output_dir());
        std::string recovered, err;
        if (!rm.reconstruct(src, recovered, err)) {
            std::cerr << "[E5] reconstruction failed: " << err << "\n";
            return 1;
        }
        std::cout << "[E5] reconstruction OK: " << recovered
                  << " is byte-identical to the original\n";
        return 0;
    }

    // ---- status ----------------------------------------------------------------------------
    int cmd_status() {
        FailureDetector fd(peers_, hb_interval_, hb_timeout_, hb_threshold_);
        for (const auto& p : fd.probe_once()) {
            meta_.set_peer_status(p.id, p.state);
            std::cout << "  peer " << p.id << " " << peer_state_str(p.state) << "\n";
        }
        return 0;
    }

    // ---- reset --------------------------------------------------------------------------------
    int cmd_reset() {
        meta_.reset();
        std::error_code ec;
        for (const auto& p : peers_)
            for (const auto& e : fs::directory_iterator(p.storage_dir(), ec))
                fs::remove(e.path(), ec);
        fs::remove_all(output_dir(), ec);
        fs::create_directories(output_dir());
        std::cout << "reset done (metadata, peer storage, output cleared)\n";
        return 0;
    }

    // ---- full Gate-1 demonstration (spec §23) --------------------------------------------------
    int cmd_demo(const std::string& file_arg) {
        const std::string file = file_arg.empty() ? "input/sample_file.bin" : file_arg;
        auto step = [](const std::string& s) {
            std::cout << "\n==== " << s << " ====\n";
        };
        step("0. start / verify the 3 peer processes");
        if (cmd_sys_start() != 0) return 1;

        step("1. chunk file (E1)");
        if (cmd_chunk(file) != 0) return 1;

        step("2. distribute + replicate (E3)");
        if (cmd_distribute() != 0) return 1;

        step("3. verify all peers ONLINE (E2 REGISTER)");
        if (cmd_ping() != 0) return 1;

        step("4. simulate random single-peer failure");
        // inline to capture the randomly selected peer id
        std::string failed;
        {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<size_t> pick(0, peers_.size() - 1);
            failed = peers_[pick(gen)].id;
            long pid = 0;
            if (!kill_peer_process(failed, pid)) return 1;
            json::Json d = json::Json::object();
            d["peer"] = failed;
            d["pid"] = pid;
            meta_.log_event("PEER_KILLED",
                            "peer process terminated (simulated failure)", d);
            std::cout << "[demo] randomly selected peer: " << failed
                      << " (pid " << pid << ") - terminated\n";
        }

        step("5. heartbeat failure detection (E4)");
        if (cmd_detect(true) != 0) {
            std::cerr << "failure was not detected\n";
            return 1;
        }

        step("6. recover affected chunks from surviving replicas (E5)");
        int rc = cmd_recover(failed, false);
        if (rc != 0) return rc;

        step("7. reconstruct + compare with original (E5)");
        if (cmd_reconstruct("") != 0) return 1;

        std::cout << "\n==== DEMO SUCCESS: peer " << failed
                  << " failed, all chunks recovered, file reconstructed "
                     "byte-identical ====\n";
        std::cout << "(peers are still running; use 'controller sys stop' to stop them)\n";
        return 0;
    }

    // ---- Gate-1 tests T01..T07 (spec §12) ------------------------------------------------
    // In-process equivalent of tests/run_tests.sh: identical scenarios, but it
    // drives the very same code paths (sys start/stop, fail, detect, recover,
    // reconstruct) without needing bash or PowerShell, so it behaves the same
    // on Windows and Linux.
    int cmd_test(const std::vector<std::string>& only) {
        struct Capture {
            std::ostringstream buf;
            std::streambuf* o;
            std::streambuf* e;
            Capture() : o(std::cout.rdbuf(buf.rdbuf())), e(std::cerr.rdbuf(buf.rdbuf())) {}
            ~Capture() { std::cout.rdbuf(o); std::cerr.rdbuf(e); }
        };
        auto quiet = [](const std::function<int()>& f, std::string* out = nullptr) {
            Capture c;
            const int rc = f();
            if (out) *out = c.buf.str();
            return rc;
        };
        auto slurp = [](const std::string& path) {
            std::ifstream in(fs::u8path(path), std::ios::binary);
            return std::string((std::istreambuf_iterator<char>(in)),
                               std::istreambuf_iterator<char>());
        };
        auto identical = [&](const std::string& a, const std::string& b) {
            std::error_code ec;
            return fs::is_regular_file(fs::u8path(b), ec) && slurp(a) == slurp(b);
        };
        auto events_contain = [&](const std::string& needle) {
            return slurp("metadata/events.json").find(needle) != std::string::npos;
        };
        const std::string in_file = "input/test_input.bin";
        const std::string out_file = "output/recovered_test_input.bin";

        auto fresh = [&]() {
            quiet([&] { return cmd_sys_stop(); });
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            quiet([&] { return cmd_reset(); });
            if (quiet([&] { return cmd_sys_start(); }) != 0) return false;
            std::error_code ec;
            fs::create_directories("input", ec);
            std::vector<char> data(300000);
            std::random_device rd;
            std::mt19937_64 gen(rd());
            for (auto& byte : data) byte = static_cast<char>(gen() & 0xFF);
            std::ofstream out(in_file, std::ios::binary | std::ios::trunc);
            out.write(data.data(), static_cast<std::streamsize>(data.size()));
            return static_cast<bool>(out);
        };
        auto chunk_and_distribute = [&]() {
            return quiet([&] { return cmd_chunk(in_file); }) == 0 &&
                   quiet([&] { return cmd_distribute(); }) == 0;
        };
        auto fail = [&](const std::string& id) {
            return quiet([&] { return cmd_fail(id); }) == 0;
        };
        auto count_chunks = [&](const std::string& peer, const std::string& fid) {
            int n = 0;
            std::error_code ec;
            for (const auto& e : fs::directory_iterator("storage/peer_" + peer + "/chunks", ec))
                if (e.path().filename().string().rfind(fid, 0) == 0 &&
                    e.path().extension() == ".chunk") ++n;
            return n;
        };

        using Result = std::pair<bool, std::string>;
        auto t01 = [&]() -> Result {
            if (!fresh()) return {false, "could not start peers"};
            if (quiet([&] { return cmd_chunk(in_file); }) != 0) return {false, "chunking failed"};
            if (quiet([&] { return cmd_distribute(); }) != 0) return {false, "distribution failed"};
            const std::string fid = meta_.file_id();
            const int a = count_chunks("A", fid), b = count_chunks("B", fid), c = count_chunks("C", fid);
            if (a == 3 && b == 4 && c == 3)
                return {true, "round-robin placement A=3,B=4,C=3, replication factor 2"};
            return {false, "unexpected placement A=" + std::to_string(a) + ",B=" +
                               std::to_string(b) + ",C=" + std::to_string(c)};
        };
        auto t02 = [&]() -> Result {
            if (!fresh() || !chunk_and_distribute()) return {false, "setup failed"};
            if (!fail("B")) return {false, "could not fail peer B"};
            quiet([&] { return cmd_detect(true); });
            if (quiet([&] { return cmd_recover("B", false); }) != 0) return {false, "recovery failed"};
            if (quiet([&] { return cmd_reconstruct(""); }) == 0 && identical(in_file, out_file))
                return {true, "single peer failure: detected, recovered, byte-identical"};
            return {false, "reconstruction mismatch after peer B failure"};
        };
        auto t03 = [&]() -> Result {
            if (!fresh() || !chunk_and_distribute()) return {false, "setup failed"};
            if (!fail("C")) return {false, "could not fail peer C"};
            quiet([&] { return cmd_detect(true); });
            if (quiet([&] { return cmd_recover("C", true); }) != 0) return {false, "recovery failed"};
            if (!events_contain("TRANSFER_INTERRUPTED")) return {false, "no TRANSFER_INTERRUPTED event logged"};
            if (quiet([&] { return cmd_reconstruct(""); }) == 0 && identical(in_file, out_file))
                return {true, "interrupted transfer discarded, full retry succeeded"};
            return {false, "reconstruction failed after retry"};
        };
        auto t04 = [&]() -> Result {
            if (!fresh() || !chunk_and_distribute()) return {false, "setup failed"};
            const std::string path = "storage/peer_A/chunks/" + meta_.file_id() + "-C001.chunk";
            {
                // corrupt peer A's copy of chunk 1; B's copy stays intact
                std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
                if (!f) return {false, "cannot open " + path};
                f.write("CORRUPTED", 9);
            }
            if (quiet([&] { return cmd_recover("A", false); }) != 0) return {false, "recovery failed"};
            if (!events_contain("INTEGRITY_FAILURE")) return {false, "no INTEGRITY_FAILURE event logged"};
            if (quiet([&] { return cmd_reconstruct(""); }) == 0 && identical(in_file, out_file))
                return {true, "SHA-256 mismatch rejected; recovered from second source"};
            return {false, "reconstruction failed after integrity rejection"};
        };
        auto t05 = [&]() -> Result {
            if (!fresh() || !chunk_and_distribute()) return {false, "setup failed"};
            if (!fail("A") || !fail("B")) return {false, "could not fail peers"};
            quiet([&] { return cmd_detect(false, 3000); });
            const int rc = quiet([&] { return cmd_recover("A", false); });
            if (rc == 3 && events_contain("UNRECOVERABLE")) {
                if (quiet([&] { return cmd_reconstruct(""); }) != 0)
                    return {true, "no surviving source -> UNRECOVERABLE reported, reconstruction refused"};
                return {false, "reconstruction should have been refused"};
            }
            return {false, "expected UNRECOVERABLE (rc=" + std::to_string(rc) + ")"};
        };
        auto t06 = [&]() -> Result {
            if (!fresh() || !chunk_and_distribute()) return {false, "setup failed"};
            if (!fail("A")) return {false, "could not fail peer A"};
            quiet([&] { return cmd_detect(true); });
            quiet([&] { return cmd_recover("A", false); });
            const int rc = quiet([&] { return cmd_reconstruct(""); });
            if (rc == 0 && identical(in_file, out_file) &&
                events_contain("\"byte_identical\": true"))
                return {true, "reconstruction in sequence order, byte-identical to original"};
            return {false, "reconstruction comparison failed (rc=" + std::to_string(rc) + ")"};
        };
        auto t07 = [&]() -> Result {
            if (!fresh() || !chunk_and_distribute()) return {false, "setup failed"};
            if (!fail("B") || !fail("C")) return {false, "could not fail peers"};
            quiet([&] { return cmd_detect(false, 3000); });
            std::string out;
            const int rc = quiet([&] { return cmd_recover("B", false); }, &out);
            // rf=2: chunks held by B+C (C2, C5) have no surviving source; the
            // rest must still be recovered and the loss reported explicitly.
            if (rc == 3 && out.find("UNRECOVERABLE") != std::string::npos &&
                out.find("recovered (3)") != std::string::npos)
                return {true, "two-peer failure: surviving chunks recovered, rest UNRECOVERABLE"};
            return {false, "unexpected result for two-peer failure (rc=" + std::to_string(rc) + ")"};
        };

        struct Test { const char* name; std::function<Result()> run; };
        const std::vector<Test> tests = {
            {"T01", t01}, {"T02", t02}, {"T03", t03}, {"T04", t04},
            {"T05", t05}, {"T06", t06}, {"T07", t07}};

        std::cout << "================ Gate-1 test suite ================\n";
        int pass = 0, failn = 0;
        for (const auto& t : tests) {
            if (!only.empty() &&
                std::find(only.begin(), only.end(), std::string(t.name)) == only.end())
                continue;
            Result r;
            try {
                r = t.run();
            } catch (const std::exception& e) {
                r = {false, std::string("exception: ") + e.what()};
            }
            std::cout << (r.first ? "PASS " : "FAIL ") << t.name << " - " << r.second << std::endl;
            (r.first ? pass : failn)++;
        }
        quiet([&] { return cmd_sys_stop(); });
        quiet([&] { return cmd_reset(); });
        std::cout << "===================================================\n";
        std::cout << "PASS=" << pass << " FAIL=" << failn << "\n";
        return failn == 0 ? 0 : 1;
    }

private:
    json::Json config_;
    std::vector<PeerInfo> peers_;
    MetadataManager meta_{"metadata"};
    uint64_t chunk_size_ = 65536;
    int hb_interval_ = 500, hb_timeout_ = 1000, hb_threshold_ = 2;

    std::string staging_dir() const { return "output/staging"; }
    std::string output_dir() const { return "output"; }
};

static void usage() {
    std::cout <<
        "usage: controller <command> [args]\n"
        "  sys start|stop|status        start / stop / list the 3 peer processes\n"
        "  fail <A|B|C|random>          simulate a peer process failure\n"
        "  demo [file]                  full Gate-1 demonstration (default: input/sample_file.bin)\n"
        "  test [T01 .. T07]            run the Gate-1 tests\n"
        "  chunk <file>                 E1: chunk file + SHA-256 + metadata\n"
        "  distribute                   E3: round-robin + replication to peers\n"
        "  ping                         E2: REGISTER handshake with all peers\n"
        "  detect [--all] [--duration-ms N]\n"
        "                               E4: heartbeat monitor (stops at first failure)\n"
        "  recover [--failed <id>] [--simulate-interrupt]\n"
        "                               E5: recover chunks from surviving replicas\n"
        "  reconstruct [original]       E5: order, concatenate, byte-compare\n"
        "  status                       probe and show peer states\n"
        "  reset                        clear metadata, peer storage, output\n";
}

int main(int argc, char** argv) {
    const std::vector<std::string> args = proc::command_line_args(argc, argv);
    if (args.size() < 2) {
        usage();
        return 1;
    }
    const std::string cmd = args[1];
    if (cmd == "help" || cmd == "--help" || cmd == "-h") {
        usage();
        return 0;
    }
    if (!sock_init()) {
        std::cerr << "controller error: socket subsystem initialisation failed\n";
        return 1;
    }
    try {
        std::string root, err;
        if (!proc::enter_project_root(root, err)) {
            std::cerr << "controller error: " << err << "\n";
            return 1;
        }
        Controller ctl;
        const size_t n = args.size();
        if (cmd == "chunk" && n == 3) return ctl.cmd_chunk(args[2]);
        if (cmd == "distribute") return ctl.cmd_distribute();
        if (cmd == "ping") return ctl.cmd_ping();
        if (cmd == "detect") {
            bool all = false;
            long long duration_ms = 0;
            for (size_t i = 2; i < n; ++i) {
                if (args[i] == "--all") all = true;
                else if (args[i] == "--duration-ms" && i + 1 < n) duration_ms = std::atoll(args[++i].c_str());
            }
            return ctl.cmd_detect(!all, duration_ms);
        }
        if (cmd == "fail" && n == 3) return ctl.cmd_fail(args[2]);
        if (cmd == "sys" && n >= 3) {
            const std::string sub = args[2];
            if (sub == "start")  return ctl.cmd_sys_start();
            if (sub == "stop")   return ctl.cmd_sys_stop();
            if (sub == "status") return ctl.cmd_sys_status();
        }
        if (cmd == "recover") {
            std::string failed;
            bool interrupt = false;
            for (size_t i = 2; i < n; ++i) {
                if (args[i] == "--failed" && i + 1 < n) failed = args[++i];
                else if (args[i] == "--simulate-interrupt") interrupt = true;
            }
            return ctl.cmd_recover(failed, interrupt);
        }
        if (cmd == "reconstruct")
            return ctl.cmd_reconstruct(n > 2 ? args[2] : "");
        if (cmd == "demo" && n <= 3) return ctl.cmd_demo(n == 3 ? args[2] : "");
        if (cmd == "test")
            return ctl.cmd_test(std::vector<std::string>(args.begin() + 2, args.end()));
        if (cmd == "status") return ctl.cmd_status();
        if (cmd == "reset") return ctl.cmd_reset();
        usage();
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "controller error: " << e.what() << "\n";
        return 1;
    }
}
