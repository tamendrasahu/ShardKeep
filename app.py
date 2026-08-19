"""
Shardkeep — Distributed File Storage & Recovery System
--------------------------------------------------------
A runnable, single-process demonstration of core distributed-storage
concepts: chunking, replication across a 4-node cluster, load-balanced
node placement, automatic re-replication on node failure, integrity
verification, secure share links (with optional password protection,
expiry, and download limits), and a hybrid cloud backup tier.

NOTE ON SCOPE (read this before presenting it as "production"):
This runs all storage "nodes" as folders managed by ONE Flask process
rather than three separate servers on separate machines, so it's easy to
run with a single command. The node-failure simulation is a flag you
flip in the UI/API (not literally killing a process), and the "AI-based
health prediction" is a transparent, documented weighted-heuristic score
over simulated CPU/RAM/disk/network metrics — not a trained ML model.
Both are called out explicitly in the README and in this file so nobody
mistakes the simulation for the real thing.
"""

import os
import sqlite3
import hashlib
import secrets
import random
import threading
import time
import io
import urllib.request
import json
import subprocess
import zipfile
from datetime import datetime
from datetime import timedelta
from flask import Flask, request, jsonify, session, send_file, g, render_template
from werkzeug.security import generate_password_hash, check_password_hash

# Optional PostgreSQL / S3 integrations. SQLite + local backup remain the default so
# the project still runs with the original one-command local setup.
try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    psycopg = None
    dict_row = None
try:
    import boto3
except ImportError:
    boto3 = None

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "shardkeep.db")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
USE_POSTGRES = DATABASE_URL.startswith(("postgresql://", "postgres://"))
S3_BUCKET = os.getenv("S3_BUCKET", "").strip()
S3_PREFIX = os.getenv("S3_PREFIX", "shardkeep/").strip("/")
STORAGE_DIR = os.path.join(BASE_DIR, "storage")
CHUNK_SIZE = 256 * 1024  # 256 KB
NODE_CAP_BYTES = 10 * 1024 * 1024 * 1024
HEARTBEAT_INTERVAL = 2.0
HEARTBEAT_TIMEOUT = 6.0
INTEGRITY_SCAN_INTERVAL = 30.0

NODE_MODE = os.getenv("NODE_MODE", "local").strip().lower()
NODE_HOSTS = [h.strip() for h in os.getenv("NODE_HOSTS", "127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1").split(",")]
NODE_PORTS = [5001, 5002, 5003, 5004]
NODE_SECRET = os.getenv("NODE_SECRET", "shardkeep-node-demo-secret")
NODE_DEFS = [
    {"id": 0, "name": "Node 1", "addr": f"{NODE_HOSTS[0]}:{NODE_PORTS[0]}", "rack": "Rack A", "cap_bytes": NODE_CAP_BYTES},
    {"id": 1, "name": "Node 2", "addr": f"{NODE_HOSTS[1]}:{NODE_PORTS[1]}", "rack": "Rack A", "cap_bytes": NODE_CAP_BYTES},
    {"id": 2, "name": "Node 3", "addr": f"{NODE_HOSTS[2]}:{NODE_PORTS[2]}", "rack": "Rack B", "cap_bytes": NODE_CAP_BYTES},
    {"id": 3, "name": "Node 4", "addr": f"{NODE_HOSTS[3]}:{NODE_PORTS[3]}", "rack": "Rack B", "cap_bytes": NODE_CAP_BYTES},
]

app = Flask(__name__)
app.secret_key = secrets.token_hex(32)
app.permanent_session_lifetime = timedelta(days=14)

# in-memory store for demo password-reset codes: {user_id: (code, expires_ts)}
RESET_CODES = {}

# in-memory, continuously-updated simulated node health metrics
NODE_METRICS = {
    n["id"]: {"cpu": random.uniform(10, 40), "ram": random.uniform(20, 50),
               "disk": random.uniform(10, 30), "net": random.uniform(5, 25)}
    for n in NODE_DEFS
}


# ---------------------------------------------------------------- storage
def node_dir(node_id):
    d = os.path.join(STORAGE_DIR, f"node{node_id}")
    os.makedirs(d, exist_ok=True)
    return d


def cloud_dir():
    d = os.path.join(STORAGE_DIR, "cloud_backup")
    os.makedirs(d, exist_ok=True)
    return d


def chunk_path(node_id, chunk_uid):
    return os.path.join(node_dir(node_id), f"{chunk_uid}.bin")

def node_base(node_id):
    return "http://" + next(n["addr"] for n in NODE_DEFS if n["id"] == node_id)

def node_request(node_id, method, path, data=None):
    url = node_base(node_id) + path
    req = urllib.request.Request(url, data=data, method=method, headers={"X-Node-Secret": NODE_SECRET})
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.read()

def node_write(node_id, chunk_uid, data):
    if NODE_MODE == "remote":
        node_request(node_id, "PUT", f"/chunks/{chunk_uid}", data)
    else:
        with open(chunk_path(node_id, chunk_uid), "wb") as f: f.write(data)

def node_read(node_id, chunk_uid):
    if NODE_MODE == "remote":
        return node_request(node_id, "GET", f"/chunks/{chunk_uid}")
    with open(chunk_path(node_id, chunk_uid), "rb") as f: return f.read()

def node_exists(node_id, chunk_uid):
    if NODE_MODE == "remote":
        try: node_request(node_id, "HEAD", f"/chunks/{chunk_uid}"); return True
        except Exception: return False
    return os.path.exists(chunk_path(node_id, chunk_uid))

def node_delete(node_id, chunk_uid):
    if NODE_MODE == "remote":
        try: node_request(node_id, "DELETE", f"/chunks/{chunk_uid}")
        except Exception: pass
    else:
        try:
            path=chunk_path(node_id, chunk_uid)
            if os.path.exists(path): os.remove(path)
        except OSError: pass


# ---------------------------------------------------------------- database
def get_db():
    if "db" not in g:
        if USE_POSTGRES:
            if psycopg is None:
                raise RuntimeError("DATABASE_URL is set for PostgreSQL, but psycopg is not installed. Run: pip install -r requirements.txt")
            g.db = psycopg.connect(DATABASE_URL, row_factory=dict_row)
        else:
            g.db = sqlite3.connect(DB_PATH)
            g.db.row_factory = sqlite3.Row
            g.db.execute("PRAGMA foreign_keys = ON")
    return g.db

def db_execute(db, sql, params=()):
    """Execute SQL with the same ? placeholders on SQLite and PostgreSQL."""
    if USE_POSTGRES:
        sql = sql.replace("?", "%s")
    return db.execute(sql if not USE_POSTGRES else sql.replace("?", "%s"), params)

def db_insert_id(cur, db):
    if USE_POSTGRES:
        row = cur.fetchone()
        return row["id"]
    return cur.lastrowid


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    fresh = False if USE_POSTGRES else not os.path.exists(DB_PATH)
    if USE_POSTGRES:
        if psycopg is None:
            raise RuntimeError("PostgreSQL selected but psycopg is not installed.")
        db = psycopg.connect(DATABASE_URL, row_factory=dict_row)
        db_execute(db, """
        CREATE TABLE IF NOT EXISTS users(
            id BIGSERIAL PRIMARY KEY, username TEXT UNIQUE NOT NULL, email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS nodes(
            id INTEGER PRIMARY KEY, name TEXT, addr TEXT, alive INTEGER DEFAULT 1,
            rack TEXT DEFAULT 'Rack A', heartbeat_enabled INTEGER DEFAULT 1, last_heartbeat DOUBLE PRECISION DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS files(
            id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id), name TEXT NOT NULL,
            size BIGINT NOT NULL, cloud_backup INTEGER DEFAULT 0, created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS chunks(
            id BIGSERIAL PRIMARY KEY, file_id BIGINT NOT NULL REFERENCES files(id), chunk_index INTEGER NOT NULL,
            chunk_uid TEXT NOT NULL, size BIGINT NOT NULL, hash TEXT NOT NULL, primary_node INTEGER NOT NULL,
            replica_node INTEGER NOT NULL, corrupted INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS share_links(
            token TEXT PRIMARY KEY, file_id BIGINT NOT NULL REFERENCES files(id), password_hash TEXT,
            expires_at DOUBLE PRECISION, max_downloads INTEGER, download_count INTEGER DEFAULT 0,
            revoked INTEGER DEFAULT 0, created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS share_collections(
            token TEXT PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id), password_hash TEXT,
            expires_at DOUBLE PRECISION, max_downloads INTEGER, download_count INTEGER DEFAULT 0,
            revoked INTEGER DEFAULT 0, created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS share_collection_files(
            token TEXT NOT NULL REFERENCES share_collections(token) ON DELETE CASCADE,
            file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
            position INTEGER DEFAULT 0,
            PRIMARY KEY(token, file_id)
        );
        """)
        db.commit()
        node_cols = {r["column_name"] for r in db_execute(db, "SELECT column_name FROM information_schema.columns WHERE table_name='nodes'").fetchall()}
    else:
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        db.executescript("""
        CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE NOT NULL, email TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
        CREATE TABLE IF NOT EXISTS nodes(id INTEGER PRIMARY KEY, name TEXT, addr TEXT, alive INTEGER DEFAULT 1, rack TEXT DEFAULT 'Rack A', heartbeat_enabled INTEGER DEFAULT 1, last_heartbeat REAL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS files(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, name TEXT NOT NULL, size INTEGER NOT NULL, cloud_backup INTEGER DEFAULT 0, created_at TEXT DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY(user_id) REFERENCES users(id));
        CREATE TABLE IF NOT EXISTS chunks(id INTEGER PRIMARY KEY AUTOINCREMENT, file_id INTEGER NOT NULL, chunk_index INTEGER NOT NULL, chunk_uid TEXT NOT NULL, size INTEGER NOT NULL, hash TEXT NOT NULL, primary_node INTEGER NOT NULL, replica_node INTEGER NOT NULL, corrupted INTEGER DEFAULT 0, FOREIGN KEY(file_id) REFERENCES files(id));
        CREATE TABLE IF NOT EXISTS share_links(token TEXT PRIMARY KEY, file_id INTEGER NOT NULL, password_hash TEXT, expires_at REAL, max_downloads INTEGER, download_count INTEGER DEFAULT 0, revoked INTEGER DEFAULT 0, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
        CREATE TABLE IF NOT EXISTS share_collections(token TEXT PRIMARY KEY, user_id INTEGER NOT NULL, password_hash TEXT, expires_at REAL, max_downloads INTEGER, download_count INTEGER DEFAULT 0, revoked INTEGER DEFAULT 0, created_at TEXT DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY(user_id) REFERENCES users(id));
        CREATE TABLE IF NOT EXISTS share_collection_files(token TEXT NOT NULL, file_id INTEGER NOT NULL, position INTEGER DEFAULT 0, PRIMARY KEY(token, file_id), FOREIGN KEY(token) REFERENCES share_collections(token) ON DELETE CASCADE, FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE);
        """)
        node_cols = {r["name"] for r in db_execute(db, "PRAGMA table_info(nodes)").fetchall()}
    if "rack" not in node_cols:
        db_execute(db, "ALTER TABLE nodes ADD COLUMN rack TEXT DEFAULT 'Rack A'")
    if "heartbeat_enabled" not in node_cols:
        db_execute(db, "ALTER TABLE nodes ADD COLUMN heartbeat_enabled INTEGER DEFAULT 1")
    if "last_heartbeat" not in node_cols:
        db_execute(db, "ALTER TABLE nodes ADD COLUMN last_heartbeat REAL DEFAULT 0")
    for n in NODE_DEFS:
        if USE_POSTGRES:
            db_execute(db, "INSERT INTO nodes(id,name,addr,alive,rack,heartbeat_enabled,last_heartbeat) VALUES (?,?,?,?,?,?,?) ON CONFLICT (id) DO NOTHING", (n["id"],n["name"],n["addr"],1,n["rack"],1,time.time()))
        else:
            db_execute(db, "INSERT OR IGNORE INTO nodes(id,name,addr,alive,rack,heartbeat_enabled,last_heartbeat) VALUES (?,?,?,?,?,?,?)", (n["id"],n["name"],n["addr"],1,n["rack"],1,time.time()))
        db_execute(db, "UPDATE nodes SET name=?, addr=?, rack=? WHERE id=?", (n["name"], n["addr"], n["rack"], n["id"]))
    db.commit(); db.close(); return fresh


# ---------------------------------------------------------------- helpers
def current_user():
    uid = session.get("user_id")
    if not uid:
        return None
    db = get_db()
    return db_execute(db, "SELECT * FROM users WHERE id=?", (uid,)).fetchone()


def require_login():
    u = current_user()
    if not u:
        return None
    return u


def sha256_bytes(b):
    return hashlib.sha256(b).hexdigest()


def alive_node_ids(db):
    return [r["id"] for r in db_execute(db, "SELECT id FROM nodes WHERE alive=1")]


def node_load_bytes(db, node_id):
    """Total bytes this node is currently holding (as primary or replica), for load balancing."""
    row = db_execute(db, """
        SELECT COALESCE(SUM(size),0) AS s FROM chunks
        WHERE (primary_node=? OR replica_node=?) AND corrupted=0
    """, (node_id, node_id)).fetchone()
    return row["s"]


def node_risk_score(db, node_id):
    m = NODE_METRICS[node_id]
    return 0.30*m["cpu"] + 0.25*m["ram"] + 0.25*m["disk"] + 0.20*m["net"]


def placement_score(db, node_id, incoming_bytes=0):
    cap = next(n["cap_bytes"] for n in NODE_DEFS if n["id"] == node_id)
    projected = (node_load_bytes(db, node_id) + incoming_bytes) / cap
    risk = node_risk_score(db, node_id) / 100.0
    net = NODE_METRICS[node_id]["net"] / 100.0
    return 0.65*projected + 0.25*risk + 0.10*net


def pick_placement(db, incoming_bytes=0):
    """Smart load placement with rack-aware replica diversity and capacity checks."""
    alive = alive_node_ids(db)
    if len(alive) < 2:
        raise RuntimeError("At least two storage nodes must be online for replication.")
    defs = {n["id"]: n for n in NODE_DEFS}
    eligible = [nid for nid in alive
                if node_load_bytes(db, nid) + incoming_bytes <= defs[nid]["cap_bytes"]]
    if len(eligible) < 2:
        raise RuntimeError("Not enough free 10 GB node capacity for this chunk and its replica.")
    primary = min(eligible, key=lambda nid: placement_score(db, nid, incoming_bytes))
    other_rack = [nid for nid in eligible if nid != primary and defs[nid]["rack"] != defs[primary]["rack"]]
    pool = other_rack or [nid for nid in eligible if nid != primary]
    replica = min(pool, key=lambda nid: placement_score(db, nid, incoming_bytes))
    return primary, replica


def log_event(events, level, msg):
    events.append({"level": level, "msg": msg, "ts": time.strftime("%H:%M:%S")})


# ---------------------------------------------------------------- auth routes
@app.post("/api/signup")
def api_signup():
    data = request.get_json(force=True)
    username = (data.get("username") or "").strip()
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    if not username or not email or not password:
        return jsonify(error="Fill in username, email, and password."), 400
    if "@" not in email or "." not in email:
        return jsonify(error="Enter a valid email address."), 400
    db = get_db()
    exists = db_execute(db, "SELECT id FROM users WHERE username=? OR email=?", (username, email)).fetchone()
    if exists:
        return jsonify(error="That username or email is already taken."), 409
    db_execute(db, "INSERT INTO users(username,email,password_hash) VALUES (?,?,?)",
               (username, email, generate_password_hash(password)))
    db.commit()
    return jsonify(ok=True)


@app.post("/api/login")
def api_login():
    data = request.get_json(force=True)
    login_id = (data.get("login_id") or "").strip().lower()
    password = data.get("password") or ""
    remember = bool(data.get("remember"))
    db = get_db()
    user = db_execute(db, "SELECT * FROM users WHERE lower(username)=? OR lower(email)=?",
                       (login_id, login_id)).fetchone()
    if not user or not check_password_hash(user["password_hash"], password):
        return jsonify(error="Incorrect username/email or password."), 401
    session.clear()
    session["user_id"] = user["id"]
    session.permanent = remember
    return jsonify(ok=True, username=user["username"])


@app.post("/api/logout")
def api_logout():
    session.clear()
    return jsonify(ok=True)


@app.get("/api/me")
def api_me():
    u = current_user()
    if not u:
        return jsonify(loggedIn=False)
    return jsonify(loggedIn=True, username=u["username"], email=u["email"])


@app.post("/api/forgot/request")
def api_forgot_request():
    data = request.get_json(force=True)
    login_id = (data.get("login_id") or "").strip().lower()
    db = get_db()
    user = db_execute(db, "SELECT * FROM users WHERE lower(username)=? OR lower(email)=?",
                       (login_id, login_id)).fetchone()
    if not user:
        return jsonify(error="No account found with that username or email."), 404
    code = f"{random.randint(0, 999999):06d}"
    RESET_CODES[user["id"]] = (code, time.time() + 600)  # 10 min expiry
    # DEMO ONLY: a real system emails this code and never returns it in the API response.
    return jsonify(ok=True, demo_code=code)


@app.post("/api/forgot/reset")
def api_forgot_reset():
    data = request.get_json(force=True)
    login_id = (data.get("login_id") or "").strip().lower()
    code = (data.get("code") or "").strip()
    new_password = data.get("new_password") or ""
    db = get_db()
    user = db_execute(db, "SELECT * FROM users WHERE lower(username)=? OR lower(email)=?",
                       (login_id, login_id)).fetchone()
    if not user:
        return jsonify(error="No account found."), 404
    entry = RESET_CODES.get(user["id"])
    if not entry or entry[0] != code or time.time() > entry[1]:
        return jsonify(error="That code is invalid or expired."), 400
    if len(new_password) < 3:
        return jsonify(error="Choose a password with at least 3 characters."), 400
    db_execute(db, "UPDATE users SET password_hash=? WHERE id=?",
               (generate_password_hash(new_password), user["id"]))
    db.commit()
    RESET_CODES.pop(user["id"], None)
    return jsonify(ok=True)


# ---------------------------------------------------------------- node routes

def re_replicate_node_data(db, failed_node_id, events):
    affected = db_execute(db, 
        "SELECT * FROM chunks WHERE primary_node=? OR replica_node=?",
        (failed_node_id, failed_node_id)
    ).fetchall()
    defs = {n["id"]: n for n in NODE_DEFS}
    for c in affected:
        source = c["replica_node"] if c["primary_node"] == failed_node_id else c["primary_node"]
        if source == failed_node_id or not node_exists(source, c["chunk_uid"]):
            continue
        candidates = [
            nid for nid in alive_node_ids(db)
            if nid != source and nid != failed_node_id
            and node_load_bytes(db, nid) + c["size"] <= defs[nid]["cap_bytes"]
        ]
        if not candidates:
            log_event(events, "warn", f"chunk_{c['chunk_index']}: no spare capacity for re-replication.")
            continue
        other_rack = [nid for nid in candidates if defs[nid]["rack"] != defs[source]["rack"]]
        pool = other_rack or candidates
        target = min(pool, key=lambda nid: placement_score(db, nid, c["size"]))
        data = node_read(source, c["chunk_uid"])
        node_write(target, c["chunk_uid"], data)
        if c["primary_node"] == failed_node_id:
            db_execute(db, "UPDATE chunks SET primary_node=? WHERE id=?", (target, c["id"]))
        else:
            db_execute(db, "UPDATE chunks SET replica_node=? WHERE id=?", (target, c["id"]))
        db.commit()
        log_event(events, "ok", f"chunk_{c['chunk_index']} re-replicated to {defs[target]['name']} ({defs[target]['rack']}).")


@app.post("/api/heartbeat/<int:node_id>")
def api_heartbeat(node_id):
    if NODE_MODE == "remote" and request.headers.get("X-Node-Secret") != NODE_SECRET:
        return jsonify(error="Invalid node secret."), 401
    db = get_db()
    row = db_execute(db, "SELECT * FROM nodes WHERE id=?", (node_id,)).fetchone()
    if not row:
        return jsonify(error="No such node."), 404
    if not row["heartbeat_enabled"]:
        return jsonify(ok=False), 409
    db_execute(db, "UPDATE nodes SET last_heartbeat=?, alive=1 WHERE id=?", (time.time(), node_id))
    db.commit()
    return jsonify(ok=True)


@app.get("/api/nodes")
def api_nodes():
    db = get_db()
    rows = db_execute(db, "SELECT * FROM nodes ORDER BY id").fetchall()
    now = time.time()
    out = []
    for r in rows:
        m = NODE_METRICS[r["id"]]
        risk = round(node_risk_score(db, r["id"]), 1)
        out.append({
            "id": r["id"], "name": r["name"], "addr": r["addr"], "rack": r["rack"],
            "alive": bool(r["alive"]), "heartbeat_enabled": bool(r["heartbeat_enabled"]),
            "heartbeat_age": round(max(0, now-(r["last_heartbeat"] or 0)), 1),
            "used_bytes": node_load_bytes(db, r["id"]),
            "cap_bytes": NODE_DEFS[r["id"]]["cap_bytes"],
            "metrics": {k: round(v,1) for k,v in m.items()},
            "risk_score": risk,
            "risk_label": "Low" if risk < 40 else ("Medium" if risk < 70 else "High")
        })
    return jsonify(nodes=out)


@app.post("/api/nodes/<int:node_id>/toggle")
def api_toggle_node(node_id):
    if not require_login():
        return jsonify(error="Not signed in."), 401
    db = get_db()
    row = db_execute(db, "SELECT * FROM nodes WHERE id=?", (node_id,)).fetchone()
    if not row:
        return jsonify(error="No such node."), 404
    events = []
    if row["heartbeat_enabled"]:
        db_execute(db, "UPDATE nodes SET heartbeat_enabled=0 WHERE id=?", (node_id,))
        db.commit()
        log_event(events, "warn", f"{row['name']} heartbeat stopped; failure detector will wait {HEARTBEAT_TIMEOUT:.0f}s.")
    else:
        db_execute(db, "UPDATE nodes SET heartbeat_enabled=1, alive=1, last_heartbeat=? WHERE id=?",
                   (time.time(), node_id))
        db.commit()
        log_event(events, "ok", f"{row['name']} heartbeat restarted.")
    return jsonify(ok=True, events=events)


@app.post("/api/chunks/<int:chunk_id>/corrupt")
def api_corrupt_chunk(chunk_id):
    if not require_login():
        return jsonify(error="Not signed in."), 401
    db = get_db()
    c = db_execute(db, "SELECT * FROM chunks WHERE id=?", (chunk_id,)).fetchone()
    if not c:
        return jsonify(error="No such chunk."), 404
    if node_exists(c["primary_node"], c["chunk_uid"]):
        b = bytearray(node_read(c["primary_node"], c["chunk_uid"]))
        if b: b[0] ^= 0xFF
        node_write(c["primary_node"], c["chunk_uid"], bytes(b))
    db_execute(db, "UPDATE chunks SET corrupted=0 WHERE id=?", (chunk_id,))
    db.commit()
    return jsonify(ok=True, message="Primary corrupted; scheduled integrity scanner will repair it.")


def heartbeat_agent(node_id):
    """Each logical node sends an HTTP heartbeat to the master."""
    while True:
        try:
            if USE_POSTGRES:
                if psycopg is None: raise RuntimeError("psycopg is not installed")
                db = psycopg.connect(DATABASE_URL, row_factory=dict_row)
                row = db_execute(db, "SELECT heartbeat_enabled FROM nodes WHERE id=?", (node_id,)).fetchone()
            else:
                db = sqlite3.connect(DB_PATH)
                row = db_execute(db, "SELECT heartbeat_enabled FROM nodes WHERE id=?", (node_id,)).fetchone()
            db.close()
            enabled = row["heartbeat_enabled"] if row and isinstance(row, dict) else (row[0] if row else 0)
            if row and enabled:
                req = urllib.request.Request(
                    f"http://127.0.0.1:5000/api/heartbeat/{node_id}", data=b"", method="POST")
                urllib.request.urlopen(req, timeout=1.5).read()
        except Exception:
            pass
        time.sleep(HEARTBEAT_INTERVAL)


def heartbeat_detector_loop():
    while True:
        try:
            with app.app_context():
                db = get_db()
                cutoff = time.time() - HEARTBEAT_TIMEOUT
                stale = db_execute(db, 
                    "SELECT * FROM nodes WHERE alive=1 AND last_heartbeat>0 AND last_heartbeat<?",
                    (cutoff,)
                ).fetchall()
                for row in stale:
                    events = []
                    db_execute(db, "UPDATE nodes SET alive=0 WHERE id=?", (row["id"],))
                    db.commit()
                    log_event(events, "bad", f"Heartbeat timeout — {row['name']} marked OFFLINE automatically.")
                    re_replicate_node_data(db, row["id"], events)
        except Exception:
            pass
        time.sleep(1)


def integrity_scan_loop():
    """Scheduled SHA-256 verification and automatic repair from the replica."""
    while True:
        time.sleep(INTEGRITY_SCAN_INTERVAL)
        try:
            with app.app_context():
                db = get_db()
                chunks = db_execute(db, "SELECT * FROM chunks").fetchall()
                for c in chunks:
                    alive_row = db_execute(db, "SELECT alive FROM nodes WHERE id=?", (c["primary_node"],)).fetchone()
                    primary_alive = bool(alive_row["alive"] if isinstance(alive_row, dict) else alive_row[0])
                    primary_ok = primary_alive and node_exists(c["primary_node"], c["chunk_uid"])
                    if primary_ok:
                        primary_ok = sha256_bytes(node_read(c["primary_node"], c["chunk_uid"])) == c["hash"]
                    replica_ok = node_exists(c["replica_node"], c["chunk_uid"])
                    if replica_ok:
                        replica_ok = sha256_bytes(node_read(c["replica_node"], c["chunk_uid"])) == c["hash"]
                    if not primary_ok and replica_ok:
                        data = node_read(c["replica_node"], c["chunk_uid"])
                        node_write(c["primary_node"], c["chunk_uid"], data)
                        db_execute(db, "UPDATE chunks SET corrupted=0 WHERE id=?", (c["id"],))
                        db.commit()
        except Exception:
            pass


# ---------------------------------------------------------------- cloud backup helpers
def cloud_key(file_id, filename):
    safe = os.path.basename(filename).replace("\\", "_")
    return f"{S3_PREFIX}/{file_id}_{safe}" if S3_PREFIX else f"{file_id}_{safe}"

def cloud_put(file_id, filename, data):
    if S3_BUCKET:
        if boto3 is None:
            raise RuntimeError("S3_BUCKET is configured but boto3 is not installed.")
        boto3.client("s3").put_object(Bucket=S3_BUCKET, Key=cloud_key(file_id, filename), Body=data)
        return "s3"
    with open(os.path.join(cloud_dir(), f"{file_id}_{os.path.basename(filename)}"), "wb") as f:
        f.write(data)
    return "local"

def cloud_get(file_id, filename):
    if S3_BUCKET:
        if boto3 is None:
            return None
        try:
            obj = boto3.client("s3").get_object(Bucket=S3_BUCKET, Key=cloud_key(file_id, filename))
            return obj["Body"].read()
        except Exception:
            return None
    path = os.path.join(cloud_dir(), f"{file_id}_{os.path.basename(filename)}")
    return open(path, "rb").read() if os.path.exists(path) else None

def cloud_delete(file_id, filename):
    if S3_BUCKET and boto3 is not None:
        try:
            boto3.client("s3").delete_object(Bucket=S3_BUCKET, Key=cloud_key(file_id, filename))
        except Exception:
            pass
    else:
        path = os.path.join(cloud_dir(), f"{file_id}_{os.path.basename(filename)}")
        try:
            if os.path.exists(path): os.remove(path)
        except OSError: pass

# ---------------------------------------------------------------- file routes
@app.get("/api/files")
def api_files():
    u = require_login()
    if not u:
        return jsonify(error="Not signed in."), 401
    db = get_db()
    files = db_execute(db, "SELECT * FROM files WHERE user_id=? ORDER BY created_at DESC", (u["id"],)).fetchall()
    out = []
    for f in files:
        chunks = db_execute(db, "SELECT * FROM chunks WHERE file_id=?", (f["id"],)).fetchall()
        alive = set(alive_node_ids(db))
        health = "healthy"
        for c in chunks:
            p_ok = c["primary_node"] in alive and not c["corrupted"]
            r_ok = c["replica_node"] in alive
            if not p_ok and not r_ok:
                health = "critical"; break
            if not p_ok and r_ok:
                health = "degraded"
        out.append({
            "id": f["id"], "name": f["name"], "size": f["size"],
            "chunk_count": len(chunks), "cloud_backup": bool(f["cloud_backup"]), "health": health,
        })
    return jsonify(files=out)


@app.post("/api/upload")
def api_upload():
    u = require_login()
    if not u:
        return jsonify(error="Not signed in."), 401
    if "file" not in request.files:
        return jsonify(error="No file provided."), 400
    upload = request.files["file"]
    data = upload.read()
    if not upload.filename:
        return jsonify(error="File name is required."), 400
    backup_to_cloud = request.form.get("cloud_backup") == "1"
    db = get_db()
    cur = db_execute(db, "INSERT INTO files(user_id,name,size,cloud_backup) VALUES (?,?,?,?)" + (" RETURNING id" if USE_POSTGRES else ""),
                     (u["id"], upload.filename, len(data), int(backup_to_cloud)))
    file_id = db_insert_id(cur, db)
    total = len(data)
    num_chunks = max(1, (total + CHUNK_SIZE - 1) // CHUNK_SIZE)
    try:
        for i in range(num_chunks):
            piece = data[i*CHUNK_SIZE:min((i+1)*CHUNK_SIZE,total)]
            chunk_uid = secrets.token_hex(8)
            primary, replica = pick_placement(db, len(piece))
            node_write(primary, chunk_uid, piece)
            node_write(replica, chunk_uid, piece)
            db_execute(db, """INSERT INTO chunks(file_id,chunk_index,chunk_uid,size,hash,primary_node,replica_node)
                          VALUES (?,?,?,?,?,?,?)""",
                       (file_id, i, chunk_uid, len(piece), sha256_bytes(piece), primary, replica))
        db.commit()
    except RuntimeError as exc:
        db.rollback()
        db_execute(db, "DELETE FROM files WHERE id=?", (file_id,))
        db.commit()
        return jsonify(error=str(exc)), 507
    if backup_to_cloud:
        try:
            cloud_put(file_id, upload.filename, data)
        except Exception as exc:
            return jsonify(error=f"Cloud backup failed: {exc}"), 502
    return jsonify(ok=True, file_id=file_id)


@app.delete("/api/files/<int:file_id>")
def api_delete_file(file_id):
    """Delete a user's file, all chunk replicas, cloud backup, and share links."""
    u = require_login()
    if not u:
        return jsonify(error="Not signed in."), 401
    db = get_db()
    f = db_execute(db, "SELECT * FROM files WHERE id=? AND user_id=?", (file_id, u["id"])).fetchone()
    if not f:
        return jsonify(error="File not found."), 404

    chunks = db_execute(db, "SELECT * FROM chunks WHERE file_id=?", (file_id,)).fetchall()
    deleted_copies = 0
    for c in chunks:
        for node_id in {c["primary_node"], c["replica_node"]}:
            if node_exists(node_id, c["chunk_uid"]):
                node_delete(node_id, c["chunk_uid"])
                deleted_copies += 1

    # Remove all share links and collection membership for this file.
    db_execute(db, "DELETE FROM share_links WHERE file_id=?", (file_id,))
    db_execute(db, "DELETE FROM share_collection_files WHERE file_id=?", (file_id,))
    db_execute(db, "DELETE FROM share_collections WHERE token NOT IN (SELECT token FROM share_collection_files)", ())
    db_execute(db, "DELETE FROM chunks WHERE file_id=?", (file_id,))
    db_execute(db, "DELETE FROM files WHERE id=? AND user_id=?", (file_id, u["id"]))
    db.commit()

    # Remove optional hybrid-cloud backup.
    cloud_delete(file_id, f["name"])

    return jsonify(ok=True, file_id=file_id, deleted_chunks=len(chunks), deleted_copies=deleted_copies)


@app.get("/api/files/<int:file_id>")
def api_file_detail(file_id):
    u = require_login()
    if not u:
        return jsonify(error="Not signed in."), 401
    db = get_db()
    f = db_execute(db, "SELECT * FROM files WHERE id=? AND user_id=?", (file_id, u["id"])).fetchone()
    if not f:
        return jsonify(error="File not found."), 404
    chunks = db_execute(db, "SELECT * FROM chunks WHERE file_id=? ORDER BY chunk_index", (file_id,)).fetchall()
    node_names = {n["id"]: n["name"] for n in NODE_DEFS}
    return jsonify(
        file={"id": f["id"], "name": f["name"], "size": f["size"], "cloud_backup": bool(f["cloud_backup"])},
        chunks=[{
            "id": c["id"], "index": c["chunk_index"], "size": c["size"], "hash": c["hash"],
            "primary": c["primary_node"], "primary_name": node_names[c["primary_node"]],
            "replica": c["replica_node"], "replica_name": node_names[c["replica_node"]],
            "corrupted": bool(c["corrupted"]),
        } for c in chunks]
    )


def reconstruct_file(db, file_id, events):
    chunks = db_execute(db, "SELECT * FROM chunks WHERE file_id=? ORDER BY chunk_index", (file_id,)).fetchall()
    node_alive = {r["id"]: bool(r["alive"]) for r in db_execute(db, "SELECT * FROM nodes")}
    node_names = {n["id"]: n["name"] for n in NODE_DEFS}
    parts = []
    for c in chunks:
        used_source = None
        if node_alive[c["primary_node"]] and not c["corrupted"]:
            piece = node_read(c["primary_node"], c["chunk_uid"])
            if sha256_bytes(piece) == c["hash"]:
                used_source = ("primary", c["primary_node"])
            else:
                log_event(events, "bad", f"chunk_{c['chunk_index']}: integrity check FAILED on {node_names[c['primary_node']]} (hash mismatch) — falling back to replica.")
        if used_source is None:
            if not node_alive[c["primary_node"]]:
                log_event(events, "warn", f"chunk_{c['chunk_index']}: primary {node_names[c['primary_node']]} offline — trying replica.")
            if node_alive[c["replica_node"]]:
                piece = node_read(c["replica_node"], c["chunk_uid"])
                used_source = ("replica", c["replica_node"])
                log_event(events, "ok", f"chunk_{c['chunk_index']}: recovered from replica {node_names[c['replica_node']]} ✓")
            else:
                log_event(events, "bad", f"chunk_{c['chunk_index']}: no available copy (both nodes down/corrupted).")
                return None
        else:
            log_event(events, "info", f"chunk_{c['chunk_index']}: fetched from primary {node_names[c['primary_node']]} ✓")
        parts.append(piece)
    return b"".join(parts)


@app.get("/api/files/<int:file_id>/download")
def api_download(file_id):
    u = require_login()
    if not u:
        return jsonify(error="Not signed in."), 401
    db = get_db()
    f = db_execute(db, "SELECT * FROM files WHERE id=? AND user_id=?", (file_id, u["id"])).fetchone()
    if not f:
        return jsonify(error="File not found."), 404
    events = []
    data = reconstruct_file(db, file_id, events)
    if data is None and f["cloud_backup"]:
        cloud_data = cloud_get(f["id"], f["name"])
        if cloud_data is not None:
            log_event(events, "accent", "All node copies unavailable — restoring from hybrid cloud backup instead.")
            data = cloud_data
    if data is None:
        return jsonify(error="Reconstruction failed — no available copies.", events=events), 409
    return send_file(io.BytesIO(data), as_attachment=True, download_name=f["name"])


@app.post("/api/files/<int:file_id>/share")
def api_create_share(file_id):
    u = require_login()
    if not u:
        return jsonify(error="Not signed in."), 401
    db = get_db()
    f = db_execute(db, "SELECT * FROM files WHERE id=? AND user_id=?", (file_id, u["id"])).fetchone()
    if not f:
        return jsonify(error="File not found."), 404
    data = request.get_json(silent=True) or {}
    password = (data.get("password") or "").strip()
    expires_hours = data.get("expires_hours")  # e.g. 24, 168; null = never
    max_downloads = data.get("max_downloads")  # e.g. 5; null = unlimited

    token = secrets.token_urlsafe(6)
    pw_hash = generate_password_hash(password) if password else None
    expires_at = (time.time() + float(expires_hours) * 3600) if expires_hours else None
    db_execute(db, """INSERT INTO share_links(token,file_id,password_hash,expires_at,max_downloads)
                  VALUES (?,?,?,?,?)""",
               (token, file_id, pw_hash, expires_at, max_downloads))
    db.commit()
    return jsonify(ok=True, token=token, url=f"/share/{token}", password_protected=bool(password))


@app.get("/api/shares")
def api_list_shares():
    u = require_login()
    if not u:
        return jsonify(error="Not signed in."), 401
    db = get_db()
    rows = db_execute(db, """
        SELECT sl.*, f.name AS file_name FROM share_links sl
        JOIN files f ON f.id = sl.file_id
        WHERE f.user_id = ? ORDER BY sl.created_at DESC
    """, (u["id"],)).fetchall()
    out = []
    for r in rows:
        status = "revoked" if r["revoked"] else \
                 ("expired" if r["expires_at"] and time.time() > r["expires_at"] else
                  ("limit reached" if r["max_downloads"] and r["download_count"] >= r["max_downloads"] else "active"))
        out.append({
            "token": r["token"], "file_id": r["file_id"], "file_name": r["file_name"],
            "url": f"/share/{r['token']}", "password_protected": bool(r["password_hash"]),
            "expires_at": r["expires_at"], "max_downloads": r["max_downloads"],
            "download_count": r["download_count"], "status": status,
        })
    return jsonify(shares=out)


@app.post("/api/shares/<token>/revoke")
def api_revoke_share(token):
    u = require_login()
    if not u:
        return jsonify(error="Not signed in."), 401
    db = get_db()
    row = db_execute(db, """SELECT sl.* FROM share_links sl JOIN files f ON f.id=sl.file_id
                         WHERE sl.token=? AND f.user_id=?""", (token, u["id"])).fetchone()
    if not row:
        return jsonify(error="Share link not found."), 404
    db_execute(db, "UPDATE share_links SET revoked=1 WHERE token=?", (token,))
    db.commit()
    return jsonify(ok=True)



@app.post("/api/share-collection")
def api_create_share_collection():
    """Create one secure public link containing multiple files owned by the current user."""
    u = require_login()
    if not u:
        return jsonify(error="Not signed in."), 401
    data = request.get_json(silent=True) or {}
    raw_ids = data.get("file_ids") or []
    try:
        file_ids = []
        for value in raw_ids:
            fid = int(value)
            if fid not in file_ids:
                file_ids.append(fid)
    except (TypeError, ValueError):
        return jsonify(error="file_ids must contain valid file IDs."), 400
    if not file_ids:
        return jsonify(error="Select at least one file."), 400
    if len(file_ids) > 100:
        return jsonify(error="A share collection can contain at most 100 files."), 400

    db = get_db()
    placeholders = ",".join(["?"] * len(file_ids))
    rows = db_execute(db, f"SELECT id,name,size FROM files WHERE user_id=? AND id IN ({placeholders})", (u["id"], *file_ids)).fetchall()
    by_id = {int(r["id"]): r for r in rows}
    if len(by_id) != len(file_ids):
        return jsonify(error="One or more selected files were not found or do not belong to you."), 404

    password = (data.get("password") or "").strip()
    expires_hours = data.get("expires_hours")
    max_downloads = data.get("max_downloads")
    try:
        expires_at = (time.time() + float(expires_hours) * 3600) if expires_hours else None
        if expires_at is not None and expires_at <= time.time():
            raise ValueError
    except (TypeError, ValueError):
        return jsonify(error="Invalid expiry value."), 400
    try:
        max_downloads = int(max_downloads) if max_downloads not in (None, "") else None
        if max_downloads is not None and max_downloads < 1:
            raise ValueError
    except (TypeError, ValueError):
        return jsonify(error="Download limit must be a positive integer."), 400

    token = secrets.token_urlsafe(9)
    pw_hash = generate_password_hash(password) if password else None
    try:
        db_execute(db, """INSERT INTO share_collections(token,user_id,password_hash,expires_at,max_downloads)\n                       VALUES (?,?,?,?,?)""", (token, u["id"], pw_hash, expires_at, max_downloads))
        for position, fid in enumerate(file_ids):
            db_execute(db, "INSERT INTO share_collection_files(token,file_id,position) VALUES (?,?,?)", (token, fid, position))
        db.commit()
    except Exception:
        db.rollback()
        raise
    return jsonify(ok=True, token=token, url=f"/share/{token}", file_count=len(file_ids), password_protected=bool(password))


@app.get("/api/share-collections")
def api_list_share_collections():
    u = require_login()
    if not u:
        return jsonify(error="Not signed in."), 401
    db = get_db()
    rows = db_execute(db, "SELECT * FROM share_collections WHERE user_id=? ORDER BY created_at DESC", (u["id"],)).fetchall()
    out=[]
    for r in rows:
        count = db_execute(db, "SELECT COUNT(*) AS n FROM share_collection_files WHERE token=?", (r["token"],)).fetchone()["n"]
        status = "revoked" if r["revoked"] else ("expired" if r["expires_at"] and time.time() > r["expires_at"] else ("limit reached" if r["max_downloads"] and r["download_count"] >= r["max_downloads"] else "active"))
        out.append({"token":r["token"],"url":f"/share/{r['token']}","file_count":int(count),"password_protected":bool(r["password_hash"]),"expires_at":r["expires_at"],"max_downloads":r["max_downloads"],"download_count":r["download_count"],"status":status})
    return jsonify(collections=out)


@app.post("/api/share-collections/<token>/revoke")
def api_revoke_share_collection(token):
    u = require_login()
    if not u:
        return jsonify(error="Not signed in."), 401
    db = get_db()
    row = db_execute(db, "SELECT * FROM share_collections WHERE token=? AND user_id=?", (token, u["id"])).fetchone()
    if not row:
        return jsonify(error="Share collection not found."), 404
    db_execute(db, "UPDATE share_collections SET revoked=1 WHERE token=?", (token,))
    db.commit()
    return jsonify(ok=True)


def _collection_files(db, token):
    return db_execute(db, """SELECT f.* FROM share_collection_files scf JOIN files f ON f.id=scf.file_id\n                             WHERE scf.token=? ORDER BY scf.position ASC""", (token,)).fetchall()


def _collection_valid(db, token):
    link = db_execute(db, "SELECT * FROM share_collections WHERE token=?", (token,)).fetchone()
    if not link:
        return None, "This share link is invalid.", 404
    if link["revoked"]:
        return None, "This share link has been revoked by its owner.", 410
    if link["expires_at"] and time.time() > link["expires_at"]:
        return None, "This share link has expired.", 410
    if link["max_downloads"] and link["download_count"] >= link["max_downloads"]:
        return None, "This share link has reached its download limit.", 410
    return link, None, 200


def _collection_password_ok(token, link):
    if not link["password_hash"]:
        return True
    return bool(session.get(f"public_collection_{token}"))


def _collection_password_form(token, error=None):
    err_html = f'<p style="color:#f0546a;font-size:13px;margin-top:10px;">{error}</p>' if error else ''
    return f"""
    <html><head><title>Password required — Shardkeep</title><meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>body{{background:#0b0f14;color:#dbe4ee;font-family:system-ui,sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}}.card{{background:#161d26;border:1px solid #232b36;border-radius:14px;padding:32px;width:100%;max-width:360px;box-sizing:border-box}}h1{{font-size:18px;margin:0 0 6px}}p.sub{{color:#7c8b9c;font-size:13px;margin:0 0 20px}}input{{width:100%;padding:10px 12px;border-radius:8px;border:1px solid #232b36;background:#0d1218;color:#dbe4ee;font-size:13px;box-sizing:border-box}}button{{width:100%;margin-top:14px;padding:11px 0;background:linear-gradient(90deg,#5ec8d8,#7c6ff0);color:#0b0f14;border:none;border-radius:8px;font-weight:700;font-size:13.5px;cursor:pointer}}</style></head>
    <body><div class="card"><h1>🔒 Password required</h1><p class="sub">This file collection was shared with a password.</p><form method="POST" action="/share/{token}"><input type="password" name="password" placeholder="Enter password" autofocus><button type="submit">Unlock shared files</button></form>{err_html}</div></body></html>"""


def _render_collection_page(token, link, files):
    expiry = datetime.fromtimestamp(link["expires_at"]).strftime("%Y-%m-%d %H:%M") if link["expires_at"] else "Never"
    limit = link["max_downloads"] if link["max_downloads"] else "∞"
    rows_list = []
    for item in files:
        item_name = html_escape(item["name"])
        item_id = item["id"]
        size_kb = item["size"] / 1024
        rows_list.append(f'<tr><td>{item_name}</td><td>{size_kb:.1f} KB</td><td><a href="/share/{token}/file/{item_id}">Download</a></td></tr>')
    rows = "".join(rows_list)
    return f"""<!doctype html><html><head><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>Shared files — Shardkeep</title><style>body{{margin:0;background:#0b0f14;color:#dbe4ee;font-family:system-ui,sans-serif;padding:24px}}.wrap{{max-width:900px;margin:auto}}.card{{background:#161d26;border:1px solid #232b36;border-radius:14px;padding:22px}}h1{{margin-top:0;font-size:24px}}.sub{{color:#8a98a8;font-size:13px}}.actions{{display:flex;gap:10px;flex-wrap:wrap;margin:18px 0}}a{{display:inline-block;background:#5ec8d8;color:#071017;padding:10px 14px;border-radius:8px;text-decoration:none;font-weight:700}}table{{width:100%;border-collapse:collapse;margin-top:12px}}th,td{{text-align:left;padding:10px;border-bottom:1px solid #232b36;font-size:13px}}@media(max-width:600px){{body{{padding:12px}}table{{min-width:560px}}.table-wrap{{overflow-x:auto}}.actions a{{width:100%;box-sizing:border-box;text-align:center}}}}</style></head><body><div class=\"wrap\"><div class=\"card\"><h1>🔗 Shared file collection</h1><div class=\"sub\">{len(files)} file(s) · Link expires: {expiry} · Downloads: {link["download_count"]}/{limit}</div><div class=\"actions\"><a href=\"/share/{token}/download-all\">⬇ Download all files</a></div><div class=\"table-wrap\"><table><thead><tr><th>File</th><th>Size</th><th></th></tr></thead><tbody>{rows}</tbody></table></div></div></div></body></html>"""


def html_escape(value):
    import html
    return html.escape(str(value))


@app.route("/share/<token>", methods=["GET", "POST"])
def public_share_collection(token):
    db = get_db()
    collection = db_execute(db, "SELECT * FROM share_collections WHERE token=?", (token,)).fetchone()
    if collection:
        link, error, status = _collection_valid(db, token)
        if link is None:
            return error, status
        if request.method == "POST":
            supplied = request.form.get("password", "")
            if not link["password_hash"] or check_password_hash(link["password_hash"], supplied):
                session[f"public_collection_{token}"] = True
                return _render_collection_page(token, link, _collection_files(db, token))
            return _collection_password_form(token, "Incorrect password."), 401
        if not _collection_password_ok(token, link):
            return _collection_password_form(token)
        return _render_collection_page(token, link, _collection_files(db, token))

    # Preserve the existing single-file share-link behavior.
    link = db_execute(db, "SELECT * FROM share_links WHERE token=?", (token,)).fetchone()
    if not link:
        return "This share link is invalid.", 404
    if request.method == "POST":
        supplied = request.form.get("password", "")
        if link["password_hash"] and not check_password_hash(link["password_hash"], supplied):
            return _share_password_form(token, error="Incorrect password."), 401
        return _single_share_download(token, link)
    if link["revoked"]:
        return "This share link has been revoked by its owner.", 410
    if link["expires_at"] and time.time() > link["expires_at"]:
        return "This share link has expired.", 410
    if link["max_downloads"] and link["download_count"] >= link["max_downloads"]:
        return "This share link has reached its download limit.", 410
    if link["password_hash"]:
        supplied = request.args.get("password", "")
        if not supplied:
            return _share_password_form(token)
        if not check_password_hash(link["password_hash"], supplied):
            return _share_password_form(token, error="Incorrect password."), 401
    return _single_share_download(token, link)


def _single_share_download(token, link):
    db = get_db()
    f = db_execute(db, "SELECT * FROM files WHERE id=?", (link["file_id"],)).fetchone()
    if not f:
        return "This shared file no longer exists.", 404
    events = []
    data = reconstruct_file(db, f["id"], events)
    if data is None and f["cloud_backup"]:
        data = cloud_get(f["id"], f["name"])
    if data is None:
        return "This file's data is currently unavailable (all copies offline).", 503
    db_execute(db, "UPDATE share_links SET download_count = download_count + 1 WHERE token=?", (token,))
    db.commit()
    return send_file(io.BytesIO(data), as_attachment=True, download_name=f["name"])


@app.get("/share/<token>/file/<int:file_id>")
def public_collection_file(token, file_id):
    db = get_db()
    link, error, status = _collection_valid(db, token)
    if link is None:
        return error, status
    if not _collection_password_ok(token, link):
        return _collection_password_form(token)
    f = db_execute(db, "SELECT f.* FROM share_collection_files scf JOIN files f ON f.id=scf.file_id WHERE scf.token=? AND f.id=?", (token, file_id)).fetchone()
    if not f:
        return "File is not part of this shared collection.", 404
    data = reconstruct_file(db, f["id"], [])
    if data is None and f["cloud_backup"]:
        data = cloud_get(f["id"], f["name"])
    if data is None:
        return "This file's data is currently unavailable.", 503
    db_execute(db, "UPDATE share_collections SET download_count=download_count+1 WHERE token=?", (token,))
    db.commit()
    return send_file(io.BytesIO(data), as_attachment=True, download_name=f["name"])


@app.get("/share/<token>/download-all")
def public_collection_download_all(token):
    db = get_db()
    link, error, status = _collection_valid(db, token)
    if link is None:
        return error, status
    if not _collection_password_ok(token, link):
        return _collection_password_form(token)
    files = _collection_files(db, token)
    if not files:
        return "This shared collection is empty.", 410
    archive = io.BytesIO()
    used_names = set()
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for f in files:
            data = reconstruct_file(db, f["id"], [])
            if data is None and f["cloud_backup"]:
                data = cloud_get(f["id"], f["name"])
            if data is None:
                continue
            name = f["name"]
            base, ext = os.path.splitext(name)
            candidate = name
            n = 2
            while candidate in used_names:
                candidate = f"{base} ({n}){ext}"
                n += 1
            used_names.add(candidate)
            zf.writestr(candidate, data)
    if not used_names:
        return "None of the files in this collection are currently available.", 503
    archive.seek(0)
    db_execute(db, "UPDATE share_collections SET download_count=download_count+1 WHERE token=?", (token,))
    db.commit()
    return send_file(archive, as_attachment=True, download_name=f"shardkeep-share-{token}.zip", mimetype="application/zip")


def _share_password_form(token, error=None):
    err_html = f'<p style="color:#f0546a;font-size:13px;margin-top:10px;">{error}</p>' if error else ''
    return f"""
    <html><head><title>Password required — Shardkeep</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
      body{{background:#0b0f14;color:#dbe4ee;font-family:system-ui,sans-serif;display:flex;
           align-items:center;justify-content:center;min-height:100vh;margin:0;}}
      .card{{background:#161d26;border:1px solid #232b36;border-radius:14px;padding:32px;width:100%;max-width:360px;}}
      h1{{font-size:18px;margin:0 0 6px;}}
      p.sub{{color:#7c8b9c;font-size:13px;margin:0 0 20px;}}
      input{{width:100%;padding:10px 12px;border-radius:8px;border:1px solid #232b36;background:#0d1218;
            color:#dbe4ee;font-family:monospace;font-size:13px;box-sizing:border-box;}}
      button{{width:100%;margin-top:14px;padding:11px 0;background:linear-gradient(90deg,#5ec8d8,#7c6ff0);
             color:#0b0f14;border:none;border-radius:8px;font-weight:700;font-size:13.5px;cursor:pointer;}}
    </style></head>
    <body><div class="card">
      <h1>🔒 Password required</h1>
      <p class="sub">This file was shared with a password.</p>
      <form method="GET" action="/share/{token}">
        <input type="password" name="password" placeholder="Enter password" autofocus>
        <button type="submit">Unlock &amp; download</button>
      </form>
      {err_html}
    </div></body></html>
    """


@app.get("/share/<token>")
def public_share_download(token):
    db = get_db()
    link = db_execute(db, "SELECT * FROM share_links WHERE token=?", (token,)).fetchone()
    if not link:
        return "This share link is invalid.", 404
    if link["revoked"]:
        return "This share link has been revoked by its owner.", 410
    if link["expires_at"] and time.time() > link["expires_at"]:
        return "This share link has expired.", 410
    if link["max_downloads"] and link["download_count"] >= link["max_downloads"]:
        return "This share link has reached its download limit.", 410
    if link["password_hash"]:
        supplied = request.args.get("password", "")
        if not supplied:
            return _share_password_form(token)
        if not check_password_hash(link["password_hash"], supplied):
            return _share_password_form(token, error="Incorrect password."), 401

    f = db_execute(db, "SELECT * FROM files WHERE id=?", (link["file_id"],)).fetchone()
    events = []
    data = reconstruct_file(db, f["id"], events)
    if data is None and f["cloud_backup"]:
        cloud_data = cloud_get(f["id"], f["name"])
        if cloud_data is not None:
            data = cloud_data
    if data is None:
        return "This file's data is currently unavailable (all copies offline).", 503

    db_execute(db, "UPDATE share_links SET download_count = download_count + 1 WHERE token=?", (token,))
    db.commit()
    return send_file(io.BytesIO(data), as_attachment=True, download_name=f["name"])


@app.get("/api/benchmark")
def api_benchmark():
    """Return live cluster capacity, node utilization, and placement latency.

    This endpoint is read-only: benchmark placement decisions are measured
    against the current cluster state without actually storing test data.
    """
    u = require_login()
    if not u:
        return jsonify(error="Not signed in."), 401

    db = get_db()
    node_rows = db_execute(db, "SELECT * FROM nodes ORDER BY id").fetchall()

    total_capacity = sum(n["cap_bytes"] for n in NODE_DEFS)
    used_bytes = sum(node_load_bytes(db, n["id"]) for n in NODE_DEFS)

    node_data = []
    for n in NODE_DEFS:
        used = node_load_bytes(db, n["id"])
        pct = (used / n["cap_bytes"] * 100.0) if n["cap_bytes"] else 0.0
        row = next((r for r in node_rows if r["id"] == n["id"]), None)
        node_data.append({
            "id": n["id"],
            "name": n["name"],
            "rack": n["rack"],
            "used_bytes": used,
            "capacity_bytes": n["cap_bytes"],
            "utilization_pct": round(pct, 2),
            "alive": bool(row["alive"]) if row else False,
        })

    # Measure the existing placement algorithm without modifying the cluster.
    # Ten decisions are timed for each sample size so the dashboard has a
    # meaningful placement-latency number.
    placement_samples = []
    for sample_bytes in (64 * 1024, 256 * 1024, 1024 * 1024):
        start = time.perf_counter()
        successful = 0
        last_error = None
        for _ in range(10):
            try:
                pick_placement(db, sample_bytes)
                successful += 1
            except RuntimeError as exc:
                last_error = str(exc)
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        placement_samples.append({
            "sample_bytes": sample_bytes,
            "placement_ms_10x": round(elapsed_ms, 3),
            "successful_decisions": successful,
            "error": last_error if successful == 0 else None,
        })

    raw_utilization_pct = (used_bytes / total_capacity * 100.0) if total_capacity else 0.0
    cloud_mode = "Amazon S3" if S3_BUCKET else "Local"

    return jsonify({
        "total_capacity_bytes": total_capacity,
        "used_bytes": used_bytes,
        "raw_utilization_pct": round(raw_utilization_pct, 2),
        "cloud_mode": cloud_mode,
        "replication_factor": 2,
        "nodes": node_data,
        "placement_samples": placement_samples,
    })


@app.get("/api/overview")
def api_overview():
    u = require_login()
    if not u:
        return jsonify(error="Not signed in."), 401
    db = get_db()
    files = db_execute(db, "SELECT COUNT(*) c FROM files WHERE user_id=?", (u["id"],)).fetchone()["c"]
    chunks = db_execute(db, """SELECT COUNT(*) c FROM chunks ch JOIN files f ON ch.file_id=f.id
                            WHERE f.user_id=?""", (u["id"],)).fetchone()["c"]
    nodes_online = db_execute(db, "SELECT COUNT(*) c FROM nodes WHERE alive=1").fetchone()["c"]

    alive = set(alive_node_ids(db))
    file_rows = db_execute(db, "SELECT id FROM files WHERE user_id=?", (u["id"],)).fetchall()
    healthy = degraded = critical = 0
    for fr in file_rows:
        chunk_rows = db_execute(db, "SELECT * FROM chunks WHERE file_id=?", (fr["id"],)).fetchall()
        state = "healthy"
        for c in chunk_rows:
            p_ok = c["primary_node"] in alive and not c["corrupted"]
            r_ok = c["replica_node"] in alive
            if not p_ok and not r_ok:
                state = "critical"; break
            if not p_ok and r_ok:
                state = "degraded"
        if state == "healthy": healthy += 1
        elif state == "degraded": degraded += 1
        else: critical += 1

    share_rows = db_execute(db, """
        SELECT sl.* FROM share_links sl JOIN files f ON f.id=sl.file_id
        WHERE f.user_id=? AND sl.revoked=0
    """, (u["id"],)).fetchall()
    now = time.time()
    active_shares = sum(
        1 for r in share_rows
        if not (r["expires_at"] and now > r["expires_at"])
        and not (r["max_downloads"] and r["download_count"] >= r["max_downloads"])
    )

    return jsonify(files=files, chunks=chunks, replicas=chunks, nodes_online=nodes_online,
                    nodes_total=len(NODE_DEFS), active_shares=active_shares,
                    replication_health={"healthy": healthy, "degraded": degraded, "critical": critical})


# ---------------------------------------------------------------- background metric simulator
def metrics_loop():
    while True:
        for nid, m in NODE_METRICS.items():
            for key in m:
                m[key] = min(95, max(3, m[key] + random.uniform(-6, 6)))
        time.sleep(4)


# ---------------------------------------------------------------- page routes
@app.get("/")
def index():
    return render_template("index.html")


if __name__ == "__main__":
    fresh = init_db()

    threading.Thread(target=metrics_loop, daemon=True).start()
    threading.Thread(target=heartbeat_detector_loop, daemon=True).start()
    threading.Thread(target=integrity_scan_loop, daemon=True).start()

    if NODE_MODE != "remote":
        for n in NODE_DEFS:
            threading.Thread(
                target=heartbeat_agent,
                args=(n["id"],),
                daemon=True
            ).start()

    port = int(os.environ.get("PORT", 5000))

    print(f"Shardkeep starting on port {port}")
    print("4 nodes × 10 GB | rack-aware placement | HTTP heartbeats | 30s integrity scan")

    if fresh:
        print("Fresh database created — sign up for a new account from the login screen.")

    app.run(
        debug=False,
        host="0.0.0.0",
        port=port,
        threaded=True
    )