# Resilient Multi-Peer File Distribution & Recovery Engine — Gate 1

A working Gate-1 prototype of a resilient multi-peer file distribution and
recovery system.

**Demonstrated flow:** original file → fixed-size chunking → SHA-256 metadata
→ round-robin distribution → replication (factor 2) → 3 peer processes →
simulated single-peer failure → heartbeat failure detection → affected-chunk
identification → surviving-replica lookup → TCP chunk recovery → SHA-256
verification → retry on invalid/interrupted recovery → sequence ordering →
file reconstruction → byte-comparison with the original → evaluation logging.

## Architecture

Six logically separated engines (spec §3):

| Engine | Component | Responsibility |
|---|---|---|
| E1 | `engine/chunking/` | binary chunking (64 KB), IDs, sequence numbers, SHA-256, chunk metadata |
| E2 | `engine/communication/` | TCP client/server, length-prefixed JSON-header + binary-payload protocol |
| E3 | `engine/distribution/` | round-robin placement, replication factor 2 |
| E4 | `engine/failure_detection/` | heartbeat + timeout failure detection |
| E5 | `engine/recovery/` | chunk recovery from surviving replicas, reconstruction, final comparison |
| E6 | `engine/metadata/` | JSON metadata + monitoring/event logging |

The **Controller** (`controller/controller.cpp`) composes the engines via a
CLI. Each **peer** (`peer/peer.cpp`) is an independent OS process with a TCP
server and isolated storage (`storage/peer_<ID>/chunks/`).

### Wire protocol (E2)

```
+--------------------+---------------------+----------------------+
| header_len (4B BE) | JSON header (UTF-8) | raw binary payload   |
+--------------------+---------------------+----------------------+
```

Message types: `REGISTER`, `REGISTER_ACK`, `HEARTBEAT`, `HEARTBEAT_ACK`,
`STORE_CHUNK`, `STORE_ACK`, `GET_CHUNK`, `CHUNK_RESPONSE`, `TRANSFER_ERROR`.
Chunk payloads travel as raw binary; partial socket reads/writes are handled
by `send_all`/`recv_all` loops; lengths are validated; I/O uses timeouts.

### Placement (E3)

`primary = index % 3`, `replica = (primary + 1) % 3`:
`C1→A+B, C2→B+C, C3→C+A, C4→A+B, C5→B+C, …` — both locations are recorded
in `metadata/file_metadata.json`.

### Failure detection (E4)

Heartbeat every **500 ms**, ACK timeout **1000 ms**, **2** missed heartbeats
→ peer marked OFFLINE → recovery system notified. Target: detection normally
≤ 2 s; the actual detection time is **measured and logged** (typically
~0.5–1.5 s), never assumed.

### Recovery (E5)

For each required chunk: surviving replica list = recorded locations minus
offline peers → `GET_CHUNK` over TCP → payload staged to a `.tmp` file →
SHA-256 recomputed and compared with the expected digest → accept (rename)
or reject (delete + log `INTEGRITY_FAILURE` + try the next source).
Interrupted transfers (`recv` returns early/EOF) discard the temporary file
and retry the **complete** chunk — no byte-range resumption. Chunks with no
surviving source are reported `UNRECOVERABLE`, and reconstruction is refused
while any required chunk is missing.

## Build

Requirements: CMake ≥ 3.16, a C++17 compiler, OpenSSL development files.

```bash
cmake -S . -B build
cmake --build build -j
```

### Windows 11 (Visual Studio 2022 / MSVC, native)

No WSL, bash, PowerShell, cmd.exe or taskkill is involved in running the
system: the controller starts, monitors and stops the peer processes itself
with the Win32 API (`CreateProcessW`, `TerminateProcess`, named events).
Administrator rights are not needed.

Prerequisites: CMake >= 3.16, Visual Studio 2022 with the *Desktop development
with C++* workload, and OpenSSL (one of):

```powershell
# a) vcpkg (recommended; DLLs are copied next to the executables automatically)
vcpkg install openssl:x64-windows
cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE="$env:VCPKG_ROOT\scripts\buildsystems\vcpkg.cmake"

# b) a regular OpenSSL installation (e.g. the Win64 OpenSSL installer)
cmake -S . -B build -DOPENSSL_ROOT_DIR="C:\Program Files\OpenSSL-Win64"
```

Build (Release):

```powershell
cmake --build build --config Release
```

This produces `build\Release\controller.exe` and `build\Release\peer.exe`
(they must stay in the same folder; the controller launches the peer that sits
next to it). Run them from anywhere - the controller locates the project
root (the folder containing `config.json`) on its own.

```powershell
.\build\Release\controller.exe sys start      # start peers A/B/C (127.0.0.1:5001/5002/5003)
.\build\Release\controller.exe sys status     # RUNNING / STOPPED per peer, with PID
.\build\Release\controller.exe fail B         # intentionally kill peer A, B or C
.\build\Release\controller.exe demo           # full Gate-1 demonstration (starts peers itself)
.\build\Release\controller.exe sys stop       # graceful stop (event), terminate as fallback
.\build\Release\controller.exe test           # Gate-1 tests T01..T07  ->  PASS=7 FAIL=0
```

Details of the Windows process layer (`engine/process/process_manager_windows.cpp`):

* peers are started with `CreateProcessW` using an absolute executable path,
  the project root as working directory and a correctly quoted command line;
* stdin = `NUL`, stdout/stderr = `run\peer_<ID>.log`; only those handles are
  inherited (`PROC_THREAD_ATTRIBUTE_HANDLE_LIST`), so a redirected controller
  stdout never gets held open by the long-lived peers;
* PIDs are kept in `run\peer_<ID>.pid`; a PID is only acted on if the process
  image really is `peer.exe` (protects against PID reuse);
* `sys stop` signals a per-process named event for a clean shutdown and falls
  back to `TerminateProcess`; `fail` uses `TerminateProcess` to simulate a crash;
* sockets: Winsock is initialised once per process, connects use a bounded
  non-blocking connect, `TCP_NODELAY` is set, and the listener does not set
  `SO_REUSEADDR` (which on Windows would allow port hijacking).

`run_system.ps1` and `tests\run_tests.ps1` are optional one-line wrappers
around `controller.exe sys ...` / `controller.exe test`.

Random failure demonstration, step by step:

```powershell
.\build\Release\controller.exe sys start
.\build\Release\controller.exe chunk input\sample_file.bin
.\build\Release\controller.exe distribute
.\build\Release\controller.exe fail random
.\build\Release\controller.exe detect
.\build\Release\controller.exe recover --failed <A|B|C>
.\build\Release\controller.exe reconstruct
.\build\Release\controller.exe sys stop
```

Dashboard (optional, Python 3):

```powershell
python .\dashboard\dashboard.py     # then open http://127.0.0.1:8090
```

### Linux / macOS

Produces `build/peer` and `build/controller`.

> Note: the spec allows "a lightweight JSON library such as nlohmann/json".
> This project bundles `third_party/mini_json.hpp` — a self-contained
> ~200-line JSON implementation — because nlohmann/json was not fetchable in
> the offline build environment. No other external dependency is used.

## Run

```bash
./build/controller sys start  # start peers A(5001) B(5002) C(5003)  (or ./run_system start)
./build/controller sys status
./build/controller chunk input/sample_file.bin
./build/controller distribute
./build/controller ping       # REGISTER handshake
./build/controller fail random
./build/controller detect     # heartbeat detection, measured time
./build/controller recover --failed <A|B|C>
./build/controller reconstruct
./build/controller sys stop
```

Or the whole demonstration in one command:

```bash
./build/controller demo        # starts the peers itself; optional: demo <file>
```

`demo` output ends with the measured detection time, affected chunks,
recovery sources, retry/integrity counters and `DEMO SUCCESS` only when the
recovered file is byte-identical to the original.

## Dashboard (visualization/control only)

```bash
python3 dashboard/dashboard.py    # http://127.0.0.1:8090
```

Shows selected file, chunk count, replication factor, peer A/B/C status,
chunk placement, failed peer, affected chunks, recovery sources, SHA-256
verification, retry count, recovery progress, measured detection time and
final reconstruction status. Buttons: Start System, Select File, Distribute
File, Simulate Random Peer Failure, Recover, Reconstruct, Reset. The
dashboard contains no recovery algorithm — it only invokes controller
commands and renders `metadata/*.json`.

## Tests (T01–T07)

```bash
cmake --build build -j
./build/controller test        # native, identical on Windows and Linux
bash tests/run_tests.sh        # same scenarios as a shell script (Linux)
ctest --test-dir build         # runs `controller test` through CTest
```

| Test | Scenario | Expected |
|---|---|---|
| T01 | normal distribution | round-robin placement, 2 copies per chunk |
| T02 | single peer failure | detected, recovered, byte-identical |
| T03 | interrupted transfer | partial data discarded, full retry succeeds |
| T04 | SHA-256 mismatch | corrupt copy rejected, second source used |
| T05 | no surviving source | `UNRECOVERABLE` reported, reconstruction refused |
| T06 | successful reconstruction | sequence order + byte-identical comparison |
| T07 | multiple-peer failure (not the primary demo) | surviving chunks recovered, rest explicitly UNRECOVERABLE |

## Metadata

`metadata/file_metadata.json` — file id, filename, original size, chunk size,
total chunks, and per-chunk id/sequence/size/SHA-256/peer locations.
`metadata/events.json` — monitoring events: peer kills, detection times,
recovery results, integrity failures, retries, bytes transferred,
reconstruction verdicts. `metadata/peer_status.json` — last known peer states.
E6 stores metadata only — never the file payload.

## Course mapping

- **DSA:** vectors/maps (`chunk_id → ChunkMeta`, `peer_id → PeerInfo`),
  round-robin, replica lookup, sort by sequence number.
- **Computer Networks:** TCP, IP/ports, sockets, request/response, timeouts,
  partial read/write handling.
- **Distributed Systems:** peer processes, replication, heartbeat failure
  detection, fault tolerance, recovery.
- **OS:** processes, threads (`std::thread`), filesystem, process failure
  (Win32 `TerminateProcess` / POSIX SIGKILL), isolated per-peer storage.
- **OOP:** `Chunk`, `PeerInfo`, `ChunkManager`, `TCPServer`, `TCPClient`,
  `Distributor`, `FailureDetector`, `RecoveryManager`, `MetadataManager`,
  `Controller` — composition, encapsulation, no god classes.
- **DBMS:** structured JSON metadata, metadata lookup.
- **Security/Cryptography:** SHA-256 integrity verification (OpenSSL
  primitive only).

## Limitations

- Loopback-only peers (127.0.0.1); replication factor fixed at 2;
  exactly 3 peers in Gate 1.
- With replication factor 2, a two-peer failure makes some chunks
  unrecoverable — the engine reports this explicitly instead of failing
  silently (T05/T07).
- No resumable byte-range transfer (excluded by the spec): interrupted
  chunks are re-fetched whole.
- Detection-time target (≤ 2 s) is measured, not guaranteed; scheduling
  load can delay it.
- The controller trusts peer identities on localhost; no authentication
  (out of scope for Gate 1).

## Final demo workflow

```
START → 3 peers ONLINE → select file → chunk → SHA-256 → distribute →
replicate → show placement → randomly fail one peer → heartbeat detects
failure → show affected chunks → recover from surviving replicas →
verify SHA-256 → reconstruct → compare original/recovered → SUCCESS
```
