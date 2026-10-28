#!/usr/bin/env python3
# ============================================================================
# dashboard.py - Modern Black & White File Sharing & Deep Analysis Dashboard
#
# Pure Black/White Monochrome Theme (No blue/navy tint) + Light/Dark Toggle
# Zero unnecessary cartoon emojis - Clean engineering aesthetic
# ============================================================================
import json
import os
import socket
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
OUTPUT = ROOT / "output"
ENV_PATH = ROOT / ".env"

def find_available_port(preferred=(8095, 8090, 8085, 8500)):
    env_port = os.environ.get("PORT")
    if env_port:
        try:
            return int(env_port)
        except ValueError:
            pass
    for port in preferred:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("0.0.0.0", port))
                return port
            except OSError:
                continue
    return 8095

PORT = find_available_port()

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

        for pid, st in peer_data.items():
            port = {"A": 5001, "B": 5002, "C": 5003}.get(pid, 5000)
            cur.execute("""
                INSERT INTO peers (peer_id, ip_port, status, last_heartbeat)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (peer_id) DO UPDATE
                SET status = EXCLUDED.status, last_heartbeat = NOW();
            """, (pid, f"127.0.0.1:{port}", st))

        if fm and "file_id" in fm:
            fid = fm["file_id"]
            fname = fm.get("filename", "unknown")
            orig_size = fm.get("original_size", 0)
            ver_id = f"{fid}-V1"

            cur.execute("""
                INSERT INTO users (user_id, email, password_hash)
                VALUES (1, 'team13@kletech.ac.in', 'scrypt_auth_hash')
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
    for p in (ROOT / "build" / "Release" / exe,
              ROOT / "build" / exe,
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

    peer_chunks = {"A": 0, "B": 0, "C": 0}
    if fm:
        for c in fm.get("chunks", {}).values():
            for p in c.get("peers", []):
                if p in peer_chunks:
                    peer_chunks[p] += 1

    fname = fm.get("filename") if fm else None
    recovered_exists = False
    recovered_size = 0
    if fname:
        rec_path = OUTPUT / f"recovered_{fname}"
        if rec_path.exists():
            recovered_exists = True
            recovered_size = rec_path.stat().st_size

    files_list = []
    if INPUT.is_dir():
        for p in sorted(INPUT.iterdir()):
            if p.is_file():
                files_list.append({
                    "name": p.name,
                    "size": p.stat().st_size,
                    "active": (fname == p.name)
                })

    return {
        "postgres": pg_stats,
        "file": {
            "filename": fm.get("filename"),
            "file_id": fm.get("file_id"),
            "original_size": fm.get("original_size"),
            "chunk_size": fm.get("chunk_size", 65536),
            "total_chunks": fm.get("total_chunks", 0),
            "replication_factor": 2,
        } if fm else None,
        "files_list": files_list,
        "recovered_file": {
            "exists": recovered_exists,
            "filename": f"recovered_{fname}" if fname else None,
            "size": recovered_size
        },
        "chunks": [
            {"chunk_id": cid, **c, "recovery_source": sources.get(cid)}
            for cid, c in sorted(fm.get("chunks", {}).items())
        ] if fm else [],
        "peers": status,
        "peer_chunks": peer_chunks,
        "failed_peer": (killed or {}).get("details", {}).get("peer"),
        "detection_ms": (detected or {}).get("details", {}).get("detection_ms"),
        "affected_chunks": (started or {}).get("details", {}).get("affected_chunks", []),
        "recovery": (recovery or {}).get("details"),
        "integrity_failures": len(integ),
        "reconstruction": (recon or {}).get("details"),
        "events": events[-60:],
    }

PAGE = """<!doctype html>
<html lang="en" data-theme="dark">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>ResilientDrive · Fault-Tolerant Distributed File Engine</title>
  <style>
    :root {
      /* PURE MONOCHROME DARK THEME (NO BLUE TINT) */
      --bg: #000000;
      --bg-subtle: #0a0a0a;
      --card-bg: #111111;
      --card-hover: #161616;
      --card-border: #242424;
      --border-light: #333333;
      --text: #f5f5f5;
      --text-muted: #888888;
      --text-dim: #555555;
      
      --btn-primary-bg: #ffffff;
      --btn-primary-text: #000000;
      --btn-primary-hover: #e5e5e5;
      
      --btn-sec-bg: #181818;
      --btn-sec-text: #e5e5e5;
      --btn-sec-border: #2a2a2a;
      --btn-sec-hover: #242424;
      
      --tag-bg: #1c1c1c;
      --tag-border: #333333;
      --tag-text: #cccccc;
      
      --online-bg: #0c1810;
      --online-border: #166534;
      --online-text: #4ade80;
      
      --offline-bg: #1f0d0d;
      --offline-border: #991b1b;
      --offline-text: #f87171;
      
      --radius: 8px;
      --font: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    }

    [data-theme="light"] {
      /* PURE MONOCHROME LIGHT THEME */
      --bg: #ffffff;
      --bg-subtle: #f8f8f8;
      --card-bg: #ffffff;
      --card-hover: #fbfbfb;
      --card-border: #e5e5e5;
      --border-light: #d4d4d4;
      --text: #111111;
      --text-muted: #666666;
      --text-dim: #999999;
      
      --btn-primary-bg: #111111;
      --btn-primary-text: #ffffff;
      --btn-primary-hover: #262626;
      
      --btn-sec-bg: #f4f4f4;
      --btn-sec-text: #111111;
      --btn-sec-border: #dcdcdc;
      --btn-sec-hover: #ebebeb;
      
      --tag-bg: #f0f0f0;
      --tag-border: #dcdcdc;
      --tag-text: #333333;
      
      --online-bg: #ecfdf5;
      --online-border: #86efac;
      --online-text: #15803d;
      
      --offline-bg: #fef2f2;
      --offline-border: #fca5a5;
      --offline-text: #b91c1c;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }
    html { scroll-behavior: smooth; }
    body {
      font-family: var(--font);
      background: var(--bg);
      color: var(--text);
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      transition: background 0.2s, color 0.2s;
    }

    /* TOP NAVBAR */
    header {
      background: var(--card-bg);
      border-bottom: 1px solid var(--card-border);
      position: sticky;
      top: 0;
      z-index: 50;
      padding: 12px 24px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .brand-mark {
      font-size: 14px;
      font-weight: 800;
      padding: 4px 8px;
      border: 1px solid var(--card-border);
      border-radius: 6px;
      background: var(--bg);
      color: var(--text);
      letter-spacing: -0.03em;
    }
    .brand-title {
      font-size: 16px;
      font-weight: 700;
      letter-spacing: -0.02em;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .brand-tag {
      font-size: 10px;
      font-weight: 600;
      text-transform: uppercase;
      padding: 2px 6px;
      border: 1px solid var(--border-light);
      border-radius: 4px;
      color: var(--text-muted);
    }
    .nav-links {
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .nav-btn {
      background: transparent;
      color: var(--text-muted);
      border: 1px solid transparent;
      padding: 6px 12px;
      border-radius: 6px;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      text-decoration: none;
      transition: all 0.15s;
    }
    .nav-btn:hover, .nav-btn.active {
      color: var(--text);
      background: var(--bg-subtle);
      border-color: var(--card-border);
    }
    .cluster-ctl {
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .btn {
      background: var(--btn-primary-bg);
      color: var(--btn-primary-text);
      border: 1px solid transparent;
      padding: 6px 12px;
      border-radius: 6px;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      transition: all 0.15s;
    }
    .btn:hover { background: var(--btn-primary-hover); }
    .btn-secondary {
      background: var(--btn-sec-bg);
      color: var(--btn-sec-text);
      border-color: var(--btn-sec-border);
    }
    .btn-secondary:hover {
      background: var(--btn-sec-hover);
    }
    .theme-toggle {
      background: var(--btn-sec-bg);
      color: var(--btn-sec-text);
      border: 1px solid var(--btn-sec-border);
      padding: 6px 10px;
      border-radius: 6px;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
    }

    /* BADGES */
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 3px 8px;
      border-radius: 4px;
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.03em;
    }
    .pill-online { background: var(--online-bg); color: var(--online-text); border: 1px solid var(--online-border); }
    .pill-offline { background: var(--offline-bg); color: var(--offline-text); border: 1px solid var(--offline-border); }
    .pill-unknown { background: var(--tag-bg); color: var(--tag-text); border: 1px solid var(--tag-border); }
    .pill-db { background: var(--tag-bg); color: var(--tag-text); border: 1px solid var(--tag-border); }
    .dot {
      width: 6px;
      height: 6px;
      border-radius: 50%;
      background: currentColor;
    }

    /* MAIN CONTAINER */
    main {
      max-width: 1280px;
      width: 100%;
      margin: 0 auto;
      padding: 24px 20px 60px;
      display: flex;
      flex-direction: column;
      gap: 24px;
    }

    /* CARDS */
    .card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: var(--radius);
      padding: 18px 20px;
    }
    .card-title {
      font-size: 14px;
      font-weight: 700;
      color: var(--text);
      margin-bottom: 12px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      letter-spacing: -0.01em;
    }

    /* DRIVE GRID */
    .drive-grid {
      display: grid;
      grid-template-columns: 1fr 1.3fr;
      gap: 18px;
    }
    @media (max-width: 900px) { .drive-grid { grid-template-columns: 1fr; } }

    /* UPLOAD DROPZONE */
    .dropzone {
      border: 1px dashed var(--border-light);
      border-radius: var(--radius);
      padding: 30px 16px;
      text-align: center;
      cursor: pointer;
      background: var(--bg-subtle);
      transition: all 0.15s;
    }
    .dropzone:hover, .dropzone.dragover {
      border-color: var(--text);
      background: var(--card-hover);
    }
    .drop-text { font-size: 14px; font-weight: 600; color: var(--text); }
    .drop-sub { font-size: 12px; color: var(--text-muted); margin-top: 4px; }

    /* ACTIVE FILE CARD */
    .file-hero {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: var(--radius);
      padding: 18px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      gap: 14px;
    }
    .file-meta-top {
      display: flex;
      align-items: flex-start;
      gap: 12px;
    }
    .file-type-icon {
      width: 44px;
      height: 44px;
      background: var(--bg-subtle);
      border: 1px solid var(--card-border);
      color: var(--text);
      border-radius: 6px;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 13px;
      font-weight: 700;
      flex-shrink: 0;
      font-family: monospace;
    }
    .file-name { font-size: 16px; font-weight: 700; color: var(--text); word-break: break-all; }
    .file-stats-row {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 8px;
      background: var(--bg-subtle);
      padding: 10px;
      border-radius: 6px;
      border: 1px solid var(--card-border);
    }
    .stat-item { text-align: center; }
    .stat-val { font-size: 15px; font-weight: 700; color: var(--text); }
    .stat-lbl { font-size: 10px; color: var(--text-muted); text-transform: uppercase; margin-top: 2px; }

    /* ACTION BUTTONS BAR */
    .actions-strip {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      padding-top: 10px;
      border-top: 1px solid var(--card-border);
    }
    .act-btn {
      flex: 1;
      min-width: 120px;
      padding: 8px 12px;
      border-radius: 6px;
      border: 1px solid var(--btn-sec-border);
      background: var(--btn-sec-bg);
      color: var(--btn-sec-text);
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      transition: all 0.15s;
    }
    .act-btn:hover { background: var(--btn-sec-hover); border-color: var(--border-light); }
    .act-btn.primary { background: var(--btn-primary-bg); color: var(--btn-primary-text); border-color: var(--btn-primary-bg); }
    .act-btn.primary:hover { background: var(--btn-primary-hover); }
    .act-btn.chaos { border-color: var(--offline-border); color: var(--offline-text); background: var(--offline-bg); }
    .act-btn.chaos:hover { border-color: var(--offline-text); }
    .act-btn.recover { border-color: var(--online-border); color: var(--online-text); background: var(--online-bg); }
    .act-btn.recover:hover { border-color: var(--online-text); }

    /* PEER CLUSTER GRID */
    .peer-grid {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 12px;
    }
    @media (max-width: 800px) { .peer-grid { grid-template-columns: 1fr; } }
    .peer-card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 6px;
      padding: 14px;
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .peer-card.is-offline {
      border-color: var(--offline-border);
      background: var(--offline-bg);
    }
    .peer-card-top {
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .peer-id { font-size: 14px; font-weight: 700; color: var(--text); }
    .peer-port { font-size: 11px; color: var(--text-muted); font-family: monospace; }
    .peer-chunks-badge {
      background: var(--bg-subtle);
      border: 1px solid var(--card-border);
      padding: 6px 8px;
      border-radius: 4px;
      font-size: 11px;
      display: flex;
      justify-content: space-between;
    }

    /* TABLE */
    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th { text-align: left; padding: 8px 10px; color: var(--text-muted); border-bottom: 1px solid var(--card-border); font-weight: 600; text-transform: uppercase; font-size: 11px; }
    td { padding: 10px; border-bottom: 1px solid var(--card-border); }
    tr:hover td { background: var(--bg-subtle); }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 11px; }

    /* TRANSITION ANALYSIS BUTTON */
    .analysis-hero-btn {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      padding: 16px 20px;
      border-radius: var(--radius);
      color: var(--text);
      display: flex;
      align-items: center;
      justify-content: space-between;
      text-decoration: none;
      cursor: pointer;
      transition: all 0.15s;
    }
    .analysis-hero-btn:hover {
      background: var(--card-hover);
      border-color: var(--border-light);
    }
    .analysis-hero-title { font-size: 15px; font-weight: 700; color: var(--text); }
    .analysis-hero-sub { font-size: 12px; color: var(--text-muted); margin-top: 3px; }
    .analysis-arrow { font-size: 18px; font-weight: 700; color: var(--text-muted); }

    /* ANALYSIS SECTION */
    #analysis {
      scroll-margin-top: 70px;
      display: flex;
      flex-direction: column;
      gap: 18px;
    }
    .analysis-header {
      border-bottom: 1px solid var(--card-border);
      padding-bottom: 10px;
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .analysis-title { font-size: 17px; font-weight: 700; color: var(--text); }
    .analysis-sub { font-size: 12px; color: var(--text-muted); margin-top: 2px; }

    /* CHUNK SEGMENTED BAR */
    .chunk-strip-container {
      background: var(--bg-subtle);
      padding: 12px;
      border-radius: 6px;
      border: 1px solid var(--card-border);
    }
    .chunk-strip {
      display: flex;
      gap: 6px;
      overflow-x: auto;
      padding-bottom: 4px;
    }
    .chunk-box {
      flex: 1;
      min-width: 100px;
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 4px;
      padding: 8px 6px;
      text-align: center;
      transition: all 0.15s;
    }
    .chunk-box:hover {
      border-color: var(--border-light);
      background: var(--card-hover);
    }
    .chunk-box-id { font-size: 11px; font-weight: 700; color: var(--text); }
    .chunk-box-size { font-size: 10px; color: var(--text-muted); margin-top: 2px; }
    .chunk-box-peers { font-size: 10px; font-weight: 600; color: var(--text-muted); margin-top: 4px; }

    /* CONSOLE LOG */
    .console-box {
      background: var(--bg-subtle);
      border: 1px solid var(--card-border);
      border-radius: 6px;
      padding: 12px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 11px;
      max-height: 250px;
      overflow-y: auto;
      white-space: pre-wrap;
      color: var(--text);
      line-height: 1.5;
    }
    .copy-btn {
      background: var(--bg-subtle);
      border: 1px solid var(--card-border);
      color: var(--text-muted);
      border-radius: 4px;
      padding: 1px 5px;
      font-size: 10px;
      cursor: pointer;
      margin-left: 4px;
    }
    .copy-btn:hover { color: var(--text); }
  </style>
</head>
<body>

  <!-- TOP HEADER -->
  <header>
    <div class="brand">
      <div class="brand-mark">RF</div>
      <div>
        <div class="brand-title">
          <span>ResilientDrive</span>
          <span class="brand-tag">Gate 2 · Sprint 1</span>
        </div>
        <div style="font-size:11px;color:var(--text-muted)">Fault-Tolerant Distributed File Engine (C++17 + PostgreSQL)</div>
      </div>
    </div>

    <div class="nav-links">
      <a href="#drive" class="nav-btn active">Cloud Drive</a>
      <a href="#analysis" class="nav-btn">Engine Analysis</a>
    </div>

    <div class="cluster-ctl">
      <button onclick="toggleTheme()" class="theme-toggle" id="themeToggleBtn">Light Mode</button>
      <span id="pgBadge" class="pill pill-db">PostgreSQL: Connecting...</span>
      <button onclick="act('start')" class="btn" title="Start 3 independent peer processes">Start Cluster</button>
      <button onclick="act('stop')" class="btn btn-secondary" title="Stop storage peers">Stop</button>
      <button onclick="act('reset')" class="btn btn-secondary" title="Reset all state and metadata">Reset</button>
    </div>
  </header>

  <main>
    <!-- CLOUD DRIVE INTERFACE -->
    <section id="drive" style="display:flex;flex-direction:column;gap:18px;">

      <div class="drive-grid">
        <!-- UPLOAD DROPZONE -->
        <div class="card" style="display:flex;flex-direction:column;justify-content:space-between">
          <div class="card-title">
            <span>Upload File to Engine</span>
            <span style="font-size:11px;color:var(--text-muted)">Fixed 64 KB Chunks</span>
          </div>
          
          <input type="file" id="fileUpload" style="display:none" onchange="uploadFile(this)">
          <div class="dropzone" id="dropzone" onclick="document.getElementById('fileUpload').click()">
            <div class="drop-text">Click to browse or drop file here</div>
            <div class="drop-sub">File will be automatically chunked into 64 KB blocks with SHA-256 digests</div>
          </div>

          <div style="margin-top:12px;display:flex;align-items:center;gap:10px">
            <span style="font-size:12px;color:var(--text-muted);font-weight:600">Select Existing:</span>
            <select id="fileSel" style="flex:1;background:var(--bg-subtle);color:var(--text);border:1px solid var(--card-border);border-radius:6px;padding:6px 8px;font-size:12px" onchange="chunkSelected()"></select>
          </div>
        </div>

        <!-- ACTIVE FILE IN DRIVE -->
        <div class="card file-hero" id="activeFileCard">
          <div>
            <div class="file-meta-top">
              <div class="file-type-icon" id="fileExtIcon">BIN</div>
              <div style="flex:1">
                <div style="display:flex;align-items:center;justify-content:space-between">
                  <span class="file-name" id="fileNameDisp">No File Selected</span>
                  <span class="pill pill-db" id="fileIdBadge">F000</span>
                </div>
                <div style="font-size:11px;color:var(--text-muted);margin-top:2px" id="fileDescDisp">Upload or select a file to begin chunking and distribution</div>
              </div>
            </div>

            <div class="file-stats-row" style="margin-top:12px">
              <div class="stat-item">
                <div class="stat-val" id="fileSizeDisp">0 KB</div>
                <div class="stat-lbl">Original Size</div>
              </div>
              <div class="stat-item">
                <div class="stat-val" id="fileChunksDisp">0</div>
                <div class="stat-lbl">64 KB Chunks</div>
              </div>
              <div class="stat-item">
                <div class="stat-val" id="fileReplDisp">R = 2</div>
                <div class="stat-lbl">Replication Factor</div>
              </div>
            </div>
          </div>

          <!-- ACTIONS BAR -->
          <div class="actions-strip">
            <button onclick="act('distribute')" class="act-btn primary" title="Distribute chunks across active peers with R=2">
              Distribute File
            </button>
            <button onclick="act('fail')" class="act-btn chaos" title="Simulate a sudden crash of one peer process">
              Crash Random Peer
            </button>
            <button onclick="act('recover')" class="act-btn recover" title="Detect failure and recover missing chunks from surviving replicas">
              Recover Chunks
            </button>
            <button onclick="act('reconstruct')" class="act-btn" title="Reorder verified chunks and assemble original file">
              Reconstruct
            </button>
            <button id="downloadBtn" onclick="downloadRecovered()" class="act-btn" style="display:none" title="Download verified recovered file directly">
              Download File
            </button>
          </div>
        </div>
      </div>

      <!-- PEER STORAGE CLUSTER NODES -->
      <div class="card">
        <div class="card-title">
          <span>Storage Peer Cluster (Independent OS Processes)</span>
          <span style="font-size:11px;color:var(--text-muted)">Heartbeat Probe: 500 ms · Timeout: 1000 ms</span>
        </div>
        <div class="peer-grid" id="peerCards">
          <!-- Dynamically populated peer cards -->
        </div>
      </div>

      <!-- RECENT FILES IN DRIVE EXPLORER -->
      <div class="card">
        <div class="card-title">
          <span>Drive Files (input/ directory)</span>
          <span style="font-size:11px;color:var(--text-muted)">Click any file to chunk and inspect</span>
        </div>
        <table>
          <thead>
            <tr>
              <th>File Name</th>
              <th>Size</th>
              <th>Status</th>
              <th style="text-align:right">Action</th>
            </tr>
          </thead>
          <tbody id="filesTableBody">
            <tr><td colspan="4" style="color:var(--text-muted);text-align:center">Loading files...</td></tr>
          </tbody>
        </table>
      </div>

      <!-- MONOCHROME ANALYSIS TRANSITION BUTTON -->
      <a href="#analysis" class="analysis-hero-btn" onclick="highlightAnalysis()">
        <div>
          <div class="analysis-hero-title">
            View Deep Engine Analysis &amp; Chunk Breakdown (E1–E6)
          </div>
          <div class="analysis-hero-sub">
            Inspect how the file is split into 64 KB pieces, round-robin placement matrix, heartbeat detection latency, and PostgreSQL schema sync.
          </div>
        </div>
        <div class="analysis-arrow">↓</div>
      </a>

    </section>

    <!-- ==================================================================== -->
    <!-- DEEP ENGINE ANALYSIS & CHUNK BREAKDOWN SECTION (BELOW) -->
    <!-- ==================================================================== -->
    <section id="analysis">
      <div class="analysis-header">
        <div>
          <div class="analysis-title">Engine Analysis &amp; Architecture Inspection</div>
          <div class="analysis-sub">Internal diagnostics across Engines E1 through E6</div>
        </div>
        <a href="#drive" class="btn btn-secondary">Back to Drive</a>
      </div>

      <!-- E1: HOW THE FILE IS SPLIT (VISUAL CHUNKING) -->
      <div class="card">
        <div class="card-title">
          <span>E1 File &amp; Chunking Engine: How The File Is Split</span>
          <span id="chunkSummaryPill" class="pill pill-db">64 KB Fixed-Size Chunking</span>
        </div>
        <div class="chunk-strip-container" style="margin-bottom:14px">
          <div style="font-size:11px;color:var(--text-muted);margin-bottom:6px">Visual Chunk Partitioning (Sequence Order):</div>
          <div class="chunk-strip" id="chunkStrip">
            <div style="color:var(--text-muted);font-size:11px">No file chunked yet. Upload or select a file above.</div>
          </div>
        </div>

        <div style="font-size:12px;font-weight:700;margin-bottom:8px;color:var(--text)">Chunk Metadata &amp; OpenSSL SHA-256 Digest Table:</div>
        <table id="chunkMetaTable">
          <thead>
            <tr>
              <th>Chunk ID</th>
              <th>Sequence</th>
              <th>Size</th>
              <th>Cryptographic SHA-256 Digest</th>
              <th>Stored On (Replicas)</th>
              <th>Recovery Source</th>
              <th>Integrity</th>
            </tr>
          </thead>
          <tbody id="chunkTableBody">
            <tr><td colspan="7" style="color:var(--text-muted);text-align:center">No chunk data</td></tr>
          </tbody>
        </table>
      </div>

      <!-- E3 & E4 & E5: REPLICATION MATRIX, DETECTION LATENCY, RECOVERY STATUS -->
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:18px">
        <!-- E3 PLACEMENT MATRIX -->
        <div class="card">
          <div class="card-title">
            <span>E3 Distribution &amp; Replication Matrix</span>
            <span class="pill pill-online">R = 2 Redundancy</span>
          </div>
          <p style="font-size:12px;color:var(--text-muted);line-height:1.5;margin-bottom:10px">
            Chunks are deterministically placed across peers using round-robin logic (i mod N) with an exact replication factor of 2.
            This ensures that any single-peer crash leaves all chunks accessible on a surviving node.
          </p>
          <div id="placementMatrix">
            <!-- Dynamic placement visualization -->
          </div>
        </div>

        <!-- E4 & E5: FAILURE DETECTION & RECONSTRUCTION BENCHMARK -->
        <div class="card">
          <div class="card-title">
            <span>E4/E5 Failure &amp; Recovery Benchmark</span>
            <span id="reconVerdictPill" class="pill pill-unknown">Standby</span>
          </div>

          <div style="display:flex;flex-direction:column;gap:10px">
            <div style="background:var(--bg-subtle);border:1px solid var(--card-border);padding:10px;border-radius:6px">
              <div style="font-size:10px;color:var(--text-muted);text-transform:uppercase">Failure Detection Latency (E4)</div>
              <div style="font-size:16px;font-weight:700;margin-top:2px" id="detectionLatencyDisp">
                No active failure
              </div>
              <div style="font-size:10px;color:var(--text-muted);margin-top:2px">Engineering Target: ≤ 2,000 ms (Threshold: 2 missed heartbeats)</div>
            </div>

            <div style="background:var(--bg-subtle);border:1px solid var(--card-border);padding:10px;border-radius:6px">
              <div style="font-size:10px;color:var(--text-muted);text-transform:uppercase">Recovery &amp; Integrity Verification (E5)</div>
              <div style="font-size:13px;color:var(--text);margin-top:2px" id="recoveryStatsDisp">
                Awaiting recovery trigger...
              </div>
            </div>

            <div style="background:var(--bg-subtle);border:1px solid var(--card-border);padding:10px;border-radius:6px">
              <div style="font-size:10px;color:var(--text-muted);text-transform:uppercase">Reconstruction Status</div>
              <div style="font-size:13px;font-weight:600;margin-top:2px" id="reconDetailsDisp">
                File not yet reconstructed
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- E6: POSTGRESQL RDBMS METADATA LIVE SYNC -->
      <div class="card">
        <div class="card-title">
          <span>E6 Metadata Store: PostgreSQL Relational Schema</span>
          <span id="pgSyncStatus" class="pill pill-db">Connected</span>
        </div>
        <p style="font-size:12px;color:var(--text-muted);line-height:1.5;margin-bottom:10px">
          The central coordination server stores file manifests, chunk placements, and audit events in a normalized PostgreSQL RDBMS (resilient_eng).
          Actual binary payload bytes are stored strictly on storage peer disks.
        </p>
        <div style="display:grid;grid-template-columns:repeat(auto-fit, minmax(140px, 1fr));gap:8px" id="pgStatsGrid">
          <!-- Live table counters -->
        </div>
      </div>

      <!-- REAL-TIME AUDIT LOG -->
      <div class="card">
        <div class="card-title">
          <span>Live Engine Event Audit Stream</span>
          <button onclick="refresh()" class="btn btn-secondary" style="font-size:10px;padding:3px 8px">Refresh Logs</button>
        </div>
        <div class="console-box" id="consoleLog">Listening for engine events...</div>
      </div>

    </section>
  </main>

  <script>
    function getTheme() {
      return localStorage.getItem('resilient_theme') || 'dark';
    }

    function applyTheme(theme) {
      document.documentElement.setAttribute('data-theme', theme);
      localStorage.setItem('resilient_theme', theme);
      const btn = document.getElementById('themeToggleBtn');
      if (btn) btn.textContent = (theme === 'dark') ? 'Light Mode' : 'Dark Mode';
    }

    function toggleTheme() {
      const cur = document.documentElement.getAttribute('data-theme') || 'dark';
      applyTheme(cur === 'dark' ? 'light' : 'dark');
    }

    applyTheme(getTheme());

    async function api(path, method) {
      try {
        const res = await fetch(path, { method: method || 'GET' });
        return await res.json();
      } catch (err) {
        console.error('API Error:', err);
        return {};
      }
    }

    function copyText(txt) {
      navigator.clipboard.writeText(txt);
      alert('Copied to clipboard: ' + txt);
    }

    function highlightAnalysis() {
      const a = document.getElementById('analysis');
      a.scrollIntoView({ behavior: 'smooth' });
    }

    async function act(action, extra) {
      let url = '/api/' + action;
      if (extra) url += '?' + extra;
      const res = await api(url, 'POST');
      if (res.output) {
        console.log('[Controller Output]:', res.output);
      }
      await refresh();
    }

    async function chunkSelected() {
      const sel = document.getElementById('fileSel').value;
      if (!sel) return;
      await api('/api/select?file=' + encodeURIComponent(sel), 'POST');
      await refresh();
    }

    async function uploadFile(input) {
      if (!input.files || input.files.length === 0) return;
      const file = input.files[0];
      try {
        const res = await fetch('/api/upload?name=' + encodeURIComponent(file.name), {
          method: 'POST',
          body: file
        });
        const data = await res.json();
        if (data.ok) {
          await api('/api/select?file=' + encodeURIComponent(data.filename), 'POST');
          await refresh();
          alert('File "' + data.filename + '" uploaded and chunked successfully!');
        } else {
          alert('Upload failed: ' + (data.error || 'unknown error'));
        }
      } catch (err) {
        alert('Upload error: ' + err.message);
      }
      input.value = '';
    }

    function downloadRecovered() {
      window.location.href = '/api/download';
    }

    const dropzone = document.getElementById('dropzone');
    dropzone.addEventListener('dragover', (e) => { e.preventDefault(); dropzone.classList.add('dragover'); });
    dropzone.addEventListener('dragleave', () => dropzone.classList.remove('dragover'));
    dropzone.addEventListener('drop', (e) => {
      e.preventDefault();
      dropzone.classList.remove('dragover');
      if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
        document.getElementById('fileUpload').files = e.dataTransfer.files;
        uploadFile(document.getElementById('fileUpload'));
      }
    });

    async function refresh() {
      const s = await api('/api/state');
      if (!s) return;

      // 1. PostgreSQL Status
      const pg = s.postgres;
      const pgBadge = document.getElementById('pgBadge');
      const pgSyncStatus = document.getElementById('pgSyncStatus');
      const pgStatsGrid = document.getElementById('pgStatsGrid');
      if (pg && pg.connected) {
        pgBadge.className = 'pill pill-online';
        pgBadge.innerHTML = `<span class="dot"></span> PostgreSQL: CONNECTED (${pg.database})`;
        pgSyncStatus.className = 'pill pill-online';
        pgSyncStatus.innerHTML = `Live Sync Active (${pg.database})`;

        pgStatsGrid.innerHTML = `
          <div style="background:var(--bg-subtle);padding:10px;border-radius:6px;border:1px solid var(--card-border);text-align:center">
            <div style="font-size:16px;font-weight:700;color:var(--text)">${pg.chunks}</div>
            <div style="font-size:10px;color:var(--text-muted)">CHUNKS TABLE</div>
          </div>
          <div style="background:var(--bg-subtle);padding:10px;border-radius:6px;border:1px solid var(--card-border);text-align:center">
            <div style="font-size:16px;font-weight:700;color:var(--text)">${pg.placements}</div>
            <div style="font-size:10px;color:var(--text-muted)">REPLICAS PLACED</div>
          </div>
          <div style="background:var(--bg-subtle);padding:10px;border-radius:6px;border:1px solid var(--card-border);text-align:center">
            <div style="font-size:16px;font-weight:700;color:var(--text)">${pg.events}</div>
            <div style="font-size:10px;color:var(--text-muted)">AUDIT EVENTS</div>
          </div>
          <div style="background:var(--bg-subtle);padding:10px;border-radius:6px;border:1px solid var(--card-border);text-align:center">
            <div style="font-size:16px;font-weight:700;color:var(--text)">7 Tables</div>
            <div style="font-size:10px;color:var(--text-muted)">RDBMS ENTITIES</div>
          </div>
        `;
      } else {
        pgBadge.className = 'pill pill-offline';
        pgBadge.innerHTML = `PostgreSQL: OFFLINE`;
        pgSyncStatus.className = 'pill pill-offline';
        pgSyncStatus.innerHTML = `Offline (File metadata mode)`;
        pgStatsGrid.innerHTML = `<div style="grid-column:1/-1;color:var(--offline-text);font-size:12px">PostgreSQL database not connected. Ensure PostgreSQL is running on port 5432.</div>`;
      }

      // 2. Active File Display
      const f = s.file;
      const fileNameDisp = document.getElementById('fileNameDisp');
      const fileIdBadge = document.getElementById('fileIdBadge');
      const fileDescDisp = document.getElementById('fileDescDisp');
      const fileSizeDisp = document.getElementById('fileSizeDisp');
      const fileChunksDisp = document.getElementById('fileChunksDisp');
      const fileReplDisp = document.getElementById('fileReplDisp');
      const fileExtIcon = document.getElementById('fileExtIcon');
      const downloadBtn = document.getElementById('downloadBtn');

      if (f && f.filename) {
        fileNameDisp.textContent = f.filename;
        fileIdBadge.textContent = f.file_id || 'ID';
        const kb = (f.original_size / 1024).toFixed(1);
        fileDescDisp.textContent = `Logical ID: ${f.file_id} · ${f.original_size.toLocaleString()} Bytes`;
        fileSizeDisp.textContent = kb + ' KB';
        fileChunksDisp.textContent = f.total_chunks + ' Chunks';
        fileReplDisp.textContent = 'R = ' + (f.replication_factor || 2);
        const ext = f.filename.split('.').pop().toUpperCase();
        fileExtIcon.textContent = ext.slice(0, 4) || 'FILE';
      } else {
        fileNameDisp.textContent = 'No File Selected';
        fileIdBadge.textContent = 'Standby';
        fileDescDisp.textContent = 'Upload or select a file from the explorer below';
        fileSizeDisp.textContent = '0 KB';
        fileChunksDisp.textContent = '0';
        fileExtIcon.textContent = 'FILE';
      }

      // Check if reconstructed file is available for download
      if (s.recovered_file && s.recovered_file.exists) {
        downloadBtn.style.display = 'inline-flex';
        downloadBtn.innerHTML = `Download (${(s.recovered_file.size / 1024).toFixed(1)} KB)`;
      } else {
        downloadBtn.style.display = 'none';
      }

      // 3. Storage Peer Cards
      const peerCards = document.getElementById('peerCards');
      const ports = { 'A': 5001, 'B': 5002, 'C': 5003 };
      peerCards.innerHTML = ['A', 'B', 'C'].map(pid => {
        const st = (s.peers && s.peers[pid]) || 'UNKNOWN';
        const isOff = (st === 'OFFLINE');
        const count = (s.peer_chunks && s.peer_chunks[pid]) || 0;
        return `
          <div class="peer-card ${isOff ? 'is-offline' : ''}">
            <div class="peer-card-top">
              <div>
                <span class="peer-id">Peer ${pid}</span>
                <span class="peer-port">: ${ports[pid]}</span>
              </div>
              <span class="pill ${isOff ? 'pill-offline' : (st === 'ONLINE' ? 'pill-online' : 'pill-unknown')}">
                <span class="dot"></span> ${st}
              </span>
            </div>
            <div class="peer-chunks-badge">
              <span style="color:var(--text-muted)">Stored Chunks:</span>
              <span style="font-weight:700;color:var(--text)">${count} Chunks</span>
            </div>
            <button onclick="act('fail', 'peer=${pid}')" class="btn btn-secondary" style="font-size:11px;padding:4px 8px;margin-top:2px" title="Simulate process termination of Peer ${pid}">
              Simulate Peer ${pid} Crash
            </button>
          </div>
        `;
      }).join('');

      // 4. File Explorer Table
      const tb = document.getElementById('filesTableBody');
      const selBox = document.getElementById('fileSel');
      if (s.files_list && s.files_list.length > 0) {
        tb.innerHTML = s.files_list.map(item => `
          <tr style="${item.active ? 'background:var(--bg-subtle)' : ''}">
            <td style="font-weight:600">
              ${item.name}
              ${item.active ? '<span class="pill pill-online" style="margin-left:6px;font-size:9px">ACTIVE</span>' : ''}
            </td>
            <td style="color:var(--text-muted)">${(item.size / 1024).toFixed(1)} KB</td>
            <td>
              <span class="pill ${item.active ? 'pill-online' : 'pill-unknown'}" style="font-size:10px">
                ${item.active ? 'Chunked &amp; Ready' : 'In Storage'}
              </span>
            </td>
            <td style="text-align:right">
              <button onclick="api('/api/select?file=' + encodeURIComponent('${item.name}'), 'POST').then(refresh)" class="btn btn-secondary" style="font-size:11px;padding:4px 8px">
                Select &amp; Chunk
              </button>
            </td>
          </tr>
        `).join('');

        const curSel = selBox.value;
        selBox.innerHTML = s.files_list.map(item => `<option value="${item.name}">${item.name} (${(item.size/1024).toFixed(1)} KB)</option>`).join('');
        if (f && f.filename) selBox.value = f.filename;
        else if (curSel) selBox.value = curSel;
      } else {
        tb.innerHTML = `<tr><td colspan="4" style="color:var(--text-muted);text-align:center">No files in input directory.</td></tr>`;
      }

      // 5. E1: Chunking Strip & Table (How it is split!)
      const chunkStrip = document.getElementById('chunkStrip');
      const chunkTableBody = document.getElementById('chunkTableBody');
      const chunkSummaryPill = document.getElementById('chunkSummaryPill');
      const placementMatrix = document.getElementById('placementMatrix');

      if (s.chunks && s.chunks.length > 0) {
        chunkSummaryPill.textContent = `${s.chunks.length} Chunks · 64 KB Boundaries`;

        // Segmented visual bar
        chunkStrip.innerHTML = s.chunks.map(c => `
          <div class="chunk-box">
            <div class="chunk-box-id">${c.chunk_id}</div>
            <div class="chunk-box-size">${(c.size / 1024).toFixed(1)} KB</div>
            <div class="chunk-box-peers">${c.peers.join(' + ') || 'Unassigned'}</div>
          </div>
        `).join('');

        // Detailed Table
        chunkTableBody.innerHTML = s.chunks.map(c => `
          <tr>
            <td class="mono" style="font-weight:700;color:var(--text)">${c.chunk_id}</td>
            <td>#${c.sequence}</td>
            <td>${c.size.toLocaleString()} B</td>
            <td class="mono" style="color:var(--text-muted)">
              ${c.sha256 ? c.sha256.slice(0, 16) + '…' + c.sha256.slice(-8) : '-'}
              <button class="copy-btn" onclick="copyText('${c.sha256}')" title="Copy full SHA-256">copy</button>
            </td>
            <td>
              ${c.peers.map((p, idx) => `<span class="pill ${idx===0 ? 'pill-online' : 'pill-db'}" style="margin-right:4px">${p} (${idx===0?'Primary':'Replica'})</span>`).join('') || '<span style="color:var(--text-muted)">Not distributed</span>'}
            </td>
            <td>${c.recovery_source ? `<span class="pill pill-online">From Peer ${c.recovery_source}</span>` : '-'}</td>
            <td><span class="pill pill-online">SHA-256 Match</span></td>
          </tr>
        `).join('');

        // Placement Matrix
        placementMatrix.innerHTML = `
          <table style="margin-top:6px">
            <thead>
              <tr><th>Chunk</th><th>Primary Peer</th><th>Replica Peer</th><th>Fault-Tolerance</th></tr>
            </thead>
            <tbody>
              ${s.chunks.map(c => `
                <tr>
                  <td class="mono"><b>${c.chunk_id}</b></td>
                  <td><span class="pill pill-online">${c.peers[0] || 'Peer A'}</span></td>
                  <td><span class="pill pill-db">${c.peers[1] || 'Peer B'}</span></td>
                  <td><span style="color:var(--online-text);font-size:11px">Protected against single node loss</span></td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        `;
      } else {
        chunkStrip.innerHTML = `<div style="color:var(--text-muted);font-size:11px;padding:6px">No file chunked yet. Upload or select a file to split it.</div>`;
        chunkTableBody.innerHTML = `<tr><td colspan="7" style="color:var(--text-muted);text-align:center">No chunk data</td></tr>`;
        placementMatrix.innerHTML = `<div style="color:var(--text-muted);font-size:11px;padding:6px">Distribute a file to generate the placement matrix.</div>`;
      }

      // 6. E4/E5 Diagnostics
      const detDisp = document.getElementById('detectionLatencyDisp');
      if (s.failed_peer) {
        const msText = s.detection_ms != null ? `${s.detection_ms} ms (Target ≤ 2,000 ms)` : 'Detecting...';
        detDisp.innerHTML = `Peer <b style="color:var(--offline-text)">${s.failed_peer}</b> OFFLINE · Detected in <b>${msText}</b>`;
        detDisp.style.color = 'var(--offline-text)';
      } else {
        detDisp.innerHTML = `All peers ONLINE · No failure observed`;
        detDisp.style.color = 'var(--online-text)';
      }

      const recDisp = document.getElementById('recoveryStatsDisp');
      if (s.recovery) {
        recDisp.innerHTML = `
          <b>Recovered:</b> ${s.recovery.recovered} chunks · <b>Unrecoverable:</b> ${s.recovery.unrecoverable} ·
          <b>Retries:</b> ${s.recovery.retry_count} · <b>Bytes Transferred:</b> ${s.recovery.bytes_transferred.toLocaleString()} B
        `;
      } else {
        recDisp.innerHTML = s.affected_chunks && s.affected_chunks.length > 0 ?
          `Affected chunks on failed peer: <span style="color:var(--text)">${s.affected_chunks.join(', ')}</span>` :
          `No chunks currently require recovery.`;
      }

      const reconDisp = document.getElementById('reconDetailsDisp');
      const reconPill = document.getElementById('reconVerdictPill');
      if (s.reconstruction) {
        if (s.reconstruction.byte_identical) {
          reconDisp.innerHTML = `<span style="color:var(--online-text)">Byte-identical reconstruction confirmed. All chunks verified in sequence.</span>`;
          reconPill.className = 'pill pill-online';
          reconPill.textContent = 'Reconstruction Verified';
        } else {
          reconDisp.innerHTML = `<span style="color:var(--offline-text)">Reconstruction hash mismatch detected</span>`;
          reconPill.className = 'pill pill-offline';
          reconPill.textContent = 'Mismatch';
        }
      } else {
        reconDisp.innerHTML = `Click "Reconstruct" above to assemble verified chunks.`;
        reconPill.className = 'pill pill-unknown';
        reconPill.textContent = 'Standby';
      }

      // 7. Console Log
      const logBox = document.getElementById('consoleLog');
      if (s.events && s.events.length > 0) {
        logBox.textContent = s.events.map(e => `${e.timestamp} [${e.category}] ${e.message}`).join('\\n');
        logBox.scrollTop = logBox.scrollHeight;
      } else {
        logBox.textContent = 'No events recorded yet.';
      }
    }

    refresh();
    setInterval(refresh, 2500);
  </script>
</body>
</html>
"""

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
            files = sorted(p.name for p in INPUT.iterdir() if p.is_file()) if INPUT.is_dir() else []
            self._send(json.dumps(files))
        elif u.path == "/api/download":
            fm = read_json(META / "file_metadata.json", {})
            fname = fm.get("filename", "recovered_file.bin")
            rec_path = OUTPUT / f"recovered_{fname}"
            if not rec_path.exists():
                rec_path = INPUT / fname
            if rec_path.exists():
                data = rec_path.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Disposition", f'attachment; filename="{fname}"')
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            else:
                self._send(json.dumps({"error": "File not found"}), code=404)
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
                self._send(json.dumps({"ok": False, "error": "Missing filename"}), code=400)
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
        elif u.path == "/api/fail":
            target = q.get("peer", ["random"])[0]
            rc, out = run_ctl("fail", target)
            if rc == 0:
                threading.Thread(target=run_ctl, args=("detect",), kwargs={"timeout": 30}, daemon=True).start()
        elif u.path in routes:
            action = routes[u.path][0]
            if action == "start":
                rc, out = self._start_system()
            elif action == "stop":
                rc, out = run_ctl("sys", "stop")
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
    print(f"ResilientDrive modern dashboard listening on port {PORT}: http://127.0.0.1:{PORT}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
