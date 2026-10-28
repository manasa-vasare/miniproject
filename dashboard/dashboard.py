#!/usr/bin/env python3
# ============================================================================
# dashboard.py - Lightweight web dashboard (spec §16) with PostgreSQL RDBMS
#
# Visualization/control layer. It renders metadata/events produced by
# the C++ core, synchronizes with PostgreSQL, and invokes controller commands.
#
# Run from the project root:  python dashboard/dashboard.py
# Then open:                  http://127.0.0.1:8090
# ============================================================================
import json
import os
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

try:
    import psycopg2
    import psycopg2.extras
    HAVE_PSYCOPG2 = True
except ImportError:
    HAVE_PSYCOPG2 = False

ROOT = Path(__file__).resolve().parent.parent
META = ROOT / "metadata"
INPUT = ROOT / "input"
ENV_PATH = ROOT / ".env"
PORT = 8090


def load_env():
    env = {}
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


def get_pg_conn():
    if not HAVE_PSYCOPG2:
        return None
    try:
        env = load_env()
        return psycopg2.connect(
            dbname=env.get("PGDATABASE", "resilient_eng"),
            user=env.get("PGUSER", "postgres"),
            password=env.get("PGPASSWORD", ""),
            host=env.get("PGHOST", "127.0.0.1"),
            port=int(env.get("PGPORT", "5432")),
            connect_timeout=2
        )
    except Exception:
        return None


def read_json(path, fallback):
    try:
        p = Path(path)
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        pass
    return fallback


def sync_to_postgres():
    conn = get_pg_conn()
    if not conn:
        return {"connected": False}
    try:
        cur = conn.cursor()
        fm = read_json(META / "file_metadata.json", {})
        ev_data = read_json(META / "events.json", {"events": []})
        events = ev_data.get("events", [])
        peer_data = read_json(META / "peer_status.json", {}).get("peers", {})

        # 1. Sync peers
        for pid, st in peer_data.items():
            port = {"A": 5001, "B": 5002, "C": 5003}.get(pid, 5000)
            cur.execute("""
                INSERT INTO peers (peer_id, ip_port, status, last_heartbeat)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (peer_id) DO UPDATE
                SET status = EXCLUDED.status, last_heartbeat = NOW();
            """, (pid, f"127.0.0.1:{port}", st))

        # 2. Sync file & chunks
        if fm and "file_id" in fm:
            fid = fm["file_id"]
            fname = fm.get("filename", "unknown")
            orig_size = fm.get("original_size", 0)
            ver_id = f"{fid}-V1"

            cur.execute("""
                INSERT INTO users (user_id, email, password_hash)
                VALUES (1, 'student@mmcoep.edu', 'auth_hash_gate1')
                ON CONFLICT (user_id) DO NOTHING;
            """)

            cur.execute("""
                INSERT INTO files (file_id, owner_id, filename, current_version_id)
                VALUES (%s, 1, %s, %s)
                ON CONFLICT (file_id) DO UPDATE
                SET filename = EXCLUDED.filename, current_version_id = EXCLUDED.current_version_id;
            """, (fid, fname, ver_id))

            cur.execute("""
                INSERT INTO file_versions (version_id, file_id, version_number, size, file_hash, status)
                VALUES (%s, %s, 1, %s, 'sha256_verified', 'ACTIVE')
                ON CONFLICT (version_id) DO UPDATE
                SET size = EXCLUDED.size, status = EXCLUDED.status;
            """, (ver_id, fid, orig_size))

            chunks = fm.get("chunks", {})
            for cid, c in chunks.items():
                seq = c.get("sequence", 1)
                csize = c.get("size", 0)
                csha = c.get("sha256", "")
                peers = c.get("peers", [])

                cur.execute("""
                    INSERT INTO chunks (chunk_id, version_id, sequence_no, size, sha256)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (chunk_id) DO UPDATE
                    SET sequence_no = EXCLUDED.sequence_no, size = EXCLUDED.size, sha256 = EXCLUDED.sha256;
                """, (cid, ver_id, seq, csize, csha))

                cur.execute("DELETE FROM chunk_placement WHERE chunk_id = %s;", (cid,))
                for idx, p in enumerate(peers):
                    state_lbl = "PRIMARY" if idx == 0 else "REPLICA"
                    cur.execute("""
                        INSERT INTO chunk_placement (chunk_id, peer_id, replica_state)
                        VALUES (%s, %s, %s);
                    """, (cid, p, state_lbl))

        # 3. Sync events
        if events:
            cur.execute("DELETE FROM recovery_events;")
            for ev in events:
                cat = ev.get("category", "INFO")
                msg = ev.get("message", "")
                det = ev.get("details", {})
                det_str = json.dumps(det) if det else ""
                res = f"{msg} | {det_str}" if det_str else msg
                cid = det.get("chunk_id") if isinstance(det, dict) else None
                pid = det.get("peer") or det.get("failed_peer") or det.get("source_peer") if isinstance(det, dict) else None

                cur.execute("""
                    INSERT INTO recovery_events (version_id, chunk_id, peer_id, event_type, result, event_time)
                    VALUES (%s, %s, %s, %s, %s, NOW());
                """, (None, cid, pid, cat, res))

        conn.commit()

        # Query stats
        cur.execute("SELECT count(*) FROM chunks;")
        chunk_count = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM chunk_placement;")
        placement_count = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM recovery_events;")
        event_count = cur.fetchone()[0]

        cur.close()
        conn.close()
        return {
            "connected": True,
            "database": "resilient_eng",
            "chunks": chunk_count,
            "placements": placement_count,
            "events": event_count
        }
    except Exception as e:
        if conn:
            conn.close()
        return {"connected": False, "error": str(e)}


def controller_path():
    exe = "controller.exe" if os.name == "nt" else "controller"
    for p in (ROOT / "build" / exe,
              ROOT / "build" / "Release" / exe,
              ROOT / "build" / "Debug" / exe):
        if p.exists():
            return str(p)
    return str(ROOT / "build" / exe)


CTL = controller_path()


def run_ctl(*args, timeout=30):
    try:
        r = subprocess.run([CTL, *args], cwd=ROOT,
                           capture_output=True, text=True,
                           encoding="utf-8", errors="replace",
                           timeout=timeout)
        sync_to_postgres()
        return r.returncode, (r.stdout + r.stderr).strip()
    except Exception as e:
        return 99, f"controller invocation failed: {e}"


def state():
    pg_stats = sync_to_postgres()
    fm = read_json(META / "file_metadata.json", {})
    events = read_json(META / "events.json", {"events": []}).get("events", [])
    status = read_json(META / "peer_status.json", {}).get("peers", {})

    def last(cat):
        for e in reversed(events):
            if e.get("category") == cat:
                return e
        return None

    killed = last("PEER_KILLED")
    detected = last("PEER_FAILURE_DETECTED")
    recovery = last("RECOVERY_FINISHED")
    started = last("RECOVERY_STARTED")
    recon = last("RECONSTRUCTION")
    integ = [e for e in events if e.get("category") == "INTEGRITY_FAILURE"]
    recovered_ev = [e for e in events if e.get("category") == "CHUNK_RECOVERED"]

    sources = {e["details"].get("chunk_id"): e["details"].get("source_peer")
               for e in recovered_ev if "details" in e}

    return {
        "postgres": pg_stats,
        "file": {
            "filename": fm.get("filename"),
            "file_id": fm.get("file_id"),
            "original_size": fm.get("original_size"),
            "chunk_size": fm.get("chunk_size"),
            "total_chunks": fm.get("total_chunks"),
            "replication_factor": 2,
        } if fm else None,
        "chunks": [
            {"chunk_id": cid, **c, "recovery_source": sources.get(cid)}
            for cid, c in sorted(fm.get("chunks", {}).items())
        ] if fm else [],
        "peers": status,
        "failed_peer": (killed or {}).get("details", {}).get("peer"),
        "detection_ms": (detected or {}).get("details", {}).get("detection_ms"),
        "affected_chunks": (started or {}).get("details", {}).get("affected_chunks", []),
        "recovery": (recovery or {}).get("details"),
        "integrity_failures": len(integ),
        "reconstruction": (recon or {}).get("details"),
        "events": events[-50:],
    }


PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>Resilient File Engine</title>
<style>
body{font-family:system-ui,sans-serif;background:#0f172a;color:#e2e8f0;margin:0;padding:24px}
h1{font-size:20px;display:flex;align-items:center;justify-content:space-between}
h2{font-size:15px;color:#94a3b8;margin:18px 0 8px}
.card{background:#1e293b;border-radius:10px;padding:16px;margin-bottom:14px}
button{background:#2563eb;color:#fff;border:0;border-radius:6px;padding:8px 14px;margin:3px;cursor:pointer}
button:hover{background:#1d4ed8}button:disabled{background:#475569}
.pill{display:inline-block;padding:2px 10px;border-radius:999px;font-size:12px;font-weight:600}
.ONLINE{background:#14532d;color:#86efac}.OFFLINE{background:#7f1d1d;color:#fca5a5}
.UNKNOWN{background:#334155;color:#cbd5e1}
.db-pill{background:#1e3a8a;color:#93c5fd;border:1px solid #3b82f6}
table{border-collapse:collapse;width:100%;font-size:13px}
td,th{padding:5px 8px;border-bottom:1px solid #334155;text-align:left}
.ok{color:#4ade80}.bad{color:#f87171}.mono{font-family:ui-monospace,monospace;font-size:12px}
#log{max-height:220px;overflow:auto;white-space:pre-wrap}
select{background:#0f172a;color:#e2e8f0;border:1px solid #475569;border-radius:6px;padding:6px}
</style></head><body>
<h1>
  <span>Resilient Multi-Peer File Distribution &amp; Recovery Engine</span>
  <span id="pgBadge" class="pill db-pill">PostgreSQL: Connecting...</span>
</h1>
<div class="card">
  <input type="file" id="fileUpload" style="display:none" onchange="uploadFile(this)">
  <button onclick="document.getElementById('fileUpload').click()" style="background:#059669">Upload File</button>
  <select id="fileSel"></select>
  <button onclick="act('start')">Start System</button>
  <button onclick="chunk()">Select File</button>
  <button onclick="act('distribute')">Distribute File</button>
  <button onclick="act('fail')">Simulate Random Peer Failure</button>
  <button onclick="act('recover')">Recover</button>
  <button onclick="act('reconstruct')">Reconstruct</button>
  <button onclick="act('reset')">Reset</button>
</div>
<div class="card"><h2>File</h2><div id="file">-</div></div>
<div class="card"><h2>Peers</h2><div id="peers">-</div></div>
<div class="card"><h2>Chunk placement (round-robin, replication factor 2)</h2>
<table id="chunks"><tr><th>chunk</th><th>seq</th><th>size</th><th>sha-256</th><th>locations</th><th>recovery source</th><th>verify</th></tr></table></div>
<div class="card"><h2>Failure &amp; recovery</h2><div id="rec">-</div></div>
<div class="card"><h2>Event log</h2><div id="log" class="mono">-</div></div>
<script>
async function api(p, m){const r = await fetch(p,{method:m||'GET'});return r.json();}
async function refresh(){
  const s = await api('/api/state');
  const pg = s.postgres;
  const badge = document.getElementById('pgBadge');
  if (pg && pg.connected) {
    badge.className = 'pill ONLINE';
    badge.innerHTML = `🐘 PostgreSQL: CONNECTED (${pg.database}) · ${pg.chunks} Chunks · ${pg.placements} Replicas · ${pg.events} Events`;
  } else {
    badge.className = 'pill OFFLINE';
    badge.innerHTML = `🐘 PostgreSQL: OFFLINE`;
  }

  document.getElementById('file').innerHTML = s.file ?
    `selected file: <b>${s.file.filename}</b> (${s.file.file_id}) · size ${s.file.original_size} B ·
     chunk size ${s.file.chunk_size} B · chunks <b>${s.file.total_chunks}</b> ·
     replication factor ${s.file.replication_factor}` : 'no file chunked yet';
  document.getElementById('peers').innerHTML = ['A','B','C'].map(p=>{
    const st = s.peers[p]||'UNKNOWN';
    return `Peer ${p} <span class="pill ${st}">${st}</span>`;}).join(' &nbsp; ');
  const rows = s.chunks.map(c=>`<tr><td class="mono">${c.chunk_id}</td><td>${c.sequence}</td>
    <td>${c.size}</td><td class="mono">${c.sha256.slice(0,16)}…</td>
    <td>${c.peers.join(' + ')}</td><td>${c.recovery_source||'-'}</td>
    <td class="ok">${c.recovery_source?'SHA-256 ✓':''}</td></tr>`).join('');
  document.getElementById('chunks').innerHTML =
    '<tr><th>chunk</th><th>seq</th><th>size</th><th>sha-256</th><th>locations</th><th>recovery source</th><th>verify</th></tr>'+rows;
  let r = '';
  if (s.failed_peer) r += `failed peer: <b class="bad">${s.failed_peer}</b> · `;
  if (s.detection_ms!=null) r += `detection time: <b>${s.detection_ms} ms</b> (measured) · `;
  if (s.affected_chunks.length) r += `affected chunks: ${s.affected_chunks.join(', ')} · `;
  if (s.recovery) r += `recovered ${s.recovery.recovered}, unrecoverable ${s.recovery.unrecoverable},
    retries ${s.recovery.retry_count}, integrity failures ${s.recovery.integrity_failures},
    bytes ${s.recovery.bytes_transferred} · `;
  if (s.reconstruction) r += s.reconstruction.byte_identical ?
    '<span class="ok">RECONSTRUCTION SUCCESS — byte-identical</span>' :
    '<span class="bad">RECONSTRUCTION MISMATCH</span>';
  document.getElementById('rec').innerHTML = r || '-';
  document.getElementById('log').textContent =
    s.events.map(e=>`${e.timestamp} [${e.category}] ${e.message}`).join('\\n');
}
async function act(a){
  if(a==='fail'){ await api('/api/fail','POST'); }
  else await api('/api/'+a,'POST');
  refresh();
}
async function uploadFile(input){
  if(!input.files || input.files.length === 0) return;
  const file = input.files[0];
  try {
    const res = await fetch('/api/upload?name=' + encodeURIComponent(file.name), {
      method: 'POST',
      body: file
    });
    const data = await res.json();
    if(data.ok){
      await files(data.filename);
      alert('File "' + data.filename + '" uploaded to input/ folder!');
    } else {
      alert('Upload failed: ' + (data.error || 'unknown error'));
    }
  } catch(err){
    alert('Upload error: ' + err.message);
  }
  input.value = '';
}
async function chunk(){
  const f = document.getElementById('fileSel').value;
  if(!f){ alert('Please select or upload a file first.'); return; }
  await api('/api/select?file='+encodeURIComponent(f),'POST'); refresh();
}
async function files(selected){
  const fs = await api('/api/files');
  const sel = document.getElementById('fileSel');
  sel.innerHTML = fs.map(f=>`<option value="${f}">${f}</option>`).join('');
  if(selected) sel.value = selected;
}
files(); refresh(); setInterval(refresh, 2000);
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def _send(self, body, ctype="application/json", code=200):
        data = body.encode() if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *a):
        pass

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/":
            self._send(PAGE, "text/html")
        elif u.path == "/api/state":
            self._send(json.dumps(state()))
        elif u.path == "/api/files":
            files = sorted(p.name for p in INPUT.iterdir() if p.is_file()) \
                if INPUT.is_dir() else []
            self._send(json.dumps(files))
        else:
            self._send("{}", code=404)

    def do_POST(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        routes = {
            "/api/start": ("start",), "/api/distribute": ("distribute",),
            "/api/fail": ("fail",), "/api/recover": ("recover",),
            "/api/reconstruct": ("reconstruct",), "/api/reset": ("reset",),
            "/api/stop": ("stop",),
        }
        if u.path == "/api/upload":
            name = Path(q.get("name", [""])[0]).name
            if not name:
                self._send(json.dumps({"ok": False, "error": "Missing or invalid filename"}), code=400)
                return
            try:
                length = int(self.headers.get("Content-Length", 0))
                data = self.rfile.read(length)
                INPUT.mkdir(parents=True, exist_ok=True)
                (INPUT / name).write_bytes(data)
                self._send(json.dumps({"ok": True, "filename": name}))
            except Exception as e:
                self._send(json.dumps({"ok": False, "error": str(e)}), code=500)
            return
        elif u.path == "/api/select":
            name = Path(q.get("file", [""])[0]).name
            rc, out = run_ctl("chunk", f"input/{name}")
        elif u.path in routes:
            action = routes[u.path][0]
            if action == "start":
                rc, out = self._start_system()
            elif action == "stop":
                rc, out = run_ctl("sys", "stop")
            elif action == "fail":
                rc, out = run_ctl("fail", "random")
                if rc == 0:
                    threading.Thread(target=run_ctl, args=("detect",),
                                     kwargs={"timeout": 30}, daemon=True).start()
            elif action == "recover":
                st = state()
                rc, out = run_ctl("recover", "--failed", st.get("failed_peer") or "")
            else:
                rc, out = run_ctl(action)
        else:
            self._send("{}", code=404)
            return
        self._send(json.dumps({"rc": rc, "output": out}))

    def _start_system(self):
        try:
            r = subprocess.run([CTL, "sys", "start"], cwd=ROOT,
                               capture_output=True, text=True, timeout=30)
            sync_to_postgres()
            return r.returncode, (r.stdout + r.stderr).strip()
        except Exception as e:
            return 99, str(e)


if __name__ == "__main__":
    print(f"dashboard listening on all interfaces at port {PORT}: http://127.0.0.1:{PORT}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
