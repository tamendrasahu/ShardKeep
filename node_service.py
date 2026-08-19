"""Shardkeep independent storage-node service.
Run one instance per node in Docker or separate terminals.
"""
import os, threading, time, urllib.request
from flask import Flask, request, Response, abort

NODE_ID = int(os.getenv("NODE_ID", "0"))
PORT = int(os.getenv("NODE_PORT", str(5001 + NODE_ID)))
MASTER_URL = os.getenv("MASTER_URL", "http://127.0.0.1:5000")
NODE_SECRET = os.getenv("NODE_SECRET", "shardkeep-node-demo-secret")
CAP_BYTES = 10 * 1024 * 1024 * 1024
ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "node_storage", f"node{NODE_ID}")
os.makedirs(ROOT, exist_ok=True)
app = Flask(__name__)

def auth():
    if request.headers.get("X-Node-Secret") != NODE_SECRET:
        abort(401)

def path(uid):
    if "/" in uid or "\\" in uid or uid != os.path.basename(uid): abort(400)
    return os.path.join(ROOT, uid + ".bin")

@app.get("/health")
def health():
    return {"ok": True, "node_id": NODE_ID, "capacity_bytes": CAP_BYTES}

@app.head("/chunks/<uid>")
def exists(uid):
    auth(); return ("", 200) if os.path.exists(path(uid)) else ("", 404)

@app.get("/chunks/<uid>")
def read_chunk(uid):
    auth(); p=path(uid)
    if not os.path.exists(p): abort(404)
    return Response(open(p,"rb"), mimetype="application/octet-stream")

@app.put("/chunks/<uid>")
def write_chunk(uid):
    auth(); data=request.get_data(); p=path(uid)
    used=sum(os.path.getsize(os.path.join(ROOT,f)) for f in os.listdir(ROOT) if f.endswith(".bin"))
    if not os.path.exists(p) and used + len(data) > CAP_BYTES: return {"error":"node capacity exceeded"}, 507
    with open(p,"wb") as f: f.write(data)
    return {"ok":True,"bytes":len(data)}

@app.delete("/chunks/<uid>")
def delete_chunk(uid):
    auth(); p=path(uid)
    if os.path.exists(p): os.remove(p)
    return {"ok":True}

def heartbeat():
    while True:
        try:
            req=urllib.request.Request(f"{MASTER_URL}/api/heartbeat/{NODE_ID}", data=b"", method="POST", headers={"X-Node-Secret":NODE_SECRET})
            urllib.request.urlopen(req,timeout=2).read()
        except Exception: pass
        time.sleep(2)

if __name__ == "__main__":
    threading.Thread(target=heartbeat,daemon=True).start()
    print(f"Shardkeep Node {NODE_ID+1} listening on http://0.0.0.0:{PORT} | capacity 10 GB")
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)
