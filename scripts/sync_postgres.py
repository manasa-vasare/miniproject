#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path
import psycopg2
import psycopg2.extras

ROOT = Path(__file__).resolve().parent.parent
META = ROOT / "metadata"
ENV_PATH = ROOT / ".env"

def load_env():
    env = {}
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env

def get_connection():
    env = load_env()
    return psycopg2.connect(
        dbname=env.get("PGDATABASE", "resilient_eng"),
        user=env.get("PGUSER", "postgres"),
        password=env.get("PGPASSWORD", ""),
        host=env.get("PGHOST", "127.0.0.1"),
        port=int(env.get("PGPORT", "5432"))
    )

def read_json(path, fallback):
    try:
        p = Path(path)
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"Error reading {path}: {e}", file=sys.stderr)
    return fallback

def sync_all():
    fm = read_json(META / "file_metadata.json", {})
    ev_data = read_json(META / "events.json", {"events": []})
    events = ev_data.get("events", [])
    peer_data = read_json(META / "peer_status.json", {}).get("peers", {})

    conn = get_connection()
    cur = conn.cursor()

    # 1. Sync peers
    for pid, state in peer_data.items():
        port = {"A": 5001, "B": 5002, "C": 5003}.get(pid, 5000)
        cur.execute("""
            INSERT INTO peers (peer_id, ip_port, status, last_heartbeat)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (peer_id) DO UPDATE
            SET status = EXCLUDED.status, last_heartbeat = NOW();
        """, (pid, f"127.0.0.1:{port}", state))

    # 2. Sync file & file_version & chunks
    if fm and "file_id" in fm:
        fid = fm["file_id"]
        fname = fm.get("filename", "unknown")
        orig_size = fm.get("original_size", 0)
        ver_id = f"{fid}-V1"

        # Ensure default user 1 exists
        cur.execute("""
            INSERT INTO users (user_id, email, password_hash)
            VALUES (1, 'student@mmcoep.edu', 'auth_hash_gate1')
            ON CONFLICT (user_id) DO NOTHING;
        """)

        # Insert file
        cur.execute("""
            INSERT INTO files (file_id, owner_id, filename, current_version_id)
            VALUES (%s, 1, %s, %s)
            ON CONFLICT (file_id) DO UPDATE
            SET filename = EXCLUDED.filename, current_version_id = EXCLUDED.current_version_id;
        """, (fid, fname, ver_id))

        # Insert file_version
        cur.execute("""
            INSERT INTO file_versions (version_id, file_id, version_number, size, file_hash, status)
            VALUES (%s, %s, 1, %s, 'sha256_verified', 'ACTIVE')
            ON CONFLICT (version_id) DO UPDATE
            SET size = EXCLUDED.size, status = EXCLUDED.status;
        """, (ver_id, fid, orig_size))

        # Insert chunks and placements
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

            # Delete old placement for this chunk to stay in sync
            cur.execute("DELETE FROM chunk_placement WHERE chunk_id = %s;", (cid,))
            for idx, p in enumerate(peers):
                state = "PRIMARY" if idx == 0 else "REPLICA"
                cur.execute("""
                    INSERT INTO chunk_placement (chunk_id, peer_id, replica_state)
                    VALUES (%s, %s, %s);
                """, (cid, p, state))

    # 3. Sync recovery events
    if events:
        cur.execute("DELETE FROM recovery_events;")
        for ev in events:
            cat = ev.get("category", "INFO")
            msg = ev.get("message", "")
            det = ev.get("details", {})
            det_str = json.dumps(det) if det else ""
            res = f"{msg} | {det_str}" if det_str else msg
            
            # extract chunk/peer if present
            cid = det.get("chunk_id") if isinstance(det, dict) else None
            pid = det.get("peer") or det.get("failed_peer") or det.get("source_peer") if isinstance(det, dict) else None

            cur.execute("""
                INSERT INTO recovery_events (version_id, chunk_id, peer_id, event_type, result, event_time)
                VALUES (%s, %s, %s, %s, %s, NOW());
            """, (None, cid, pid, cat, res))

    conn.commit()
    cur.close()
    conn.close()
    print("PostgreSQL synchronization successful.")

if __name__ == "__main__":
    sync_all()
