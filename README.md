# Shardkeep — Distributed File Storage & Recovery System

A runnable demonstration of core distributed-storage concepts: chunking,
replication, load-balanced placement, automatic re-replication on node
failure, integrity verification, secure share links, and hybrid cloud
backup — with real accounts, real password hashing, and a dashboard UI.

## Quick start

```bash
cd shardkeep
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

Open **http://127.0.0.1:5000**. On first run a fresh SQLite database
(`shardkeep.db`) is created automatically — sign up for a new account
from the login screen (there are no pre-seeded demo accounts this time,
since real signup now works end to end).

## Feature map

| # | Feature | How it's implemented here |
|---|---|---|
| 1 | File chunking & distributed storage | Uploads are split into 256 KB chunks and written to per-node folders under `storage/`, across a **4-node cluster** |
| 2 | Intelligent replication | Every chunk is written to a **primary** node and a **replica** node |
| 3 | Automatic node failure detection & recovery | Toggling a node "offline" triggers immediate **re-replication**: chunks anchored on that node get a fresh copy written to the least-loaded healthy node |
| 4 | Intelligent load balancing | Primary/replica placement always picks the **least-loaded currently-online node**, not a random one |
| 5 | Data integrity & corruption detection | Every chunk is SHA-256 hashed at upload; on download the hash is re-verified, and a mismatch triggers automatic fallback to the replica (there's a "simulate corruption" button per chunk so you can demo this live) |
| 6 | Node health prediction | Each node has simulated CPU/RAM/disk/network metrics that drift over time, combined into a weighted **risk score** (see honesty note below) |
| 7 | Secure file sharing via link | Generate a link with an **optional password, expiry window, and download limit**; a **Sharing Manager** view lists every link across all your files with live status and one-click **revoke** |
| 8 | Hybrid cloud backup | Optional checkbox on upload stores a full extra copy in `storage/cloud_backup/`, used as a last-resort restore path if all chunk copies are unavailable |
| 9 | Real-time dashboard | Sidebar dashboard with live stat cards, a **replication-health ring chart**, a polling node/health view, and a running activity log |
| 10 | Failure simulation & recovery demo | Per-node "simulate failure / bring back online" buttons, live in the Nodes view |

## Honest notes on scope (read before calling this "production-ready")

- **"Nodes" are folders, not machines.** All four storage nodes run
  inside one Flask process for easy local demoing. To make them real
  separate services, split `node_dir()`'s file I/O behind a tiny HTTP
  API and run four instances of that service — the master server logic
  here barely changes.
- **"AI-based" health prediction is a transparent weighted heuristic**
  (`0.30·cpu + 0.25·ram + 0.25·disk + 0.20·net`), not a trained model.
  It's a reasonable stand-in for a demo, but if you want to say "ML" on
  a resume, swap this for a scikit-learn model trained on real or
  synthetic node telemetry — the metrics collection loop in
  `metrics_loop()` is already the right place to feed it.
- **Password reset shows the code on-screen** instead of emailing it,
  because there's no mail server configured. Swap in an SMTP/SendGrid
  call in `api_forgot_request()` for the real thing.
- **Node "failure" is a database flag**, not an actual process kill —
  chunk bytes stay on disk so the demo is reversible. That's fine for
  teaching the concept; a real system would detect failure via missed
  heartbeats over the network.
- **Share-link passwords use the same hashing as account passwords**
  (werkzeug's `generate_password_hash`), and links are checked for
  revocation, expiry, and download-limit on every access — this part
  is real, not simulated.

## Project structure

```
shardkeep/
  app.py                 Flask backend: auth, chunking, replication, nodes, sharing
  requirements.txt
  templates/index.html   Single-page dashboard shell
  static/css/style.css
  static/js/app.js       All frontend logic (fetch calls to the REST API)
  storage/                Created at runtime: node0/, node1/, node2/, cloud_backup/
  shardkeep.db            Created at runtime (SQLite)
```

## Where to go next

- Split nodes into real separate processes/services communicating over HTTP
- Replace the heuristic risk score with a trained model
- Add real email delivery for password reset
- Add file versioning and deduplication (hash-match skip on upload)
- Containerize each node with Docker for a genuinely distributed demo
- If you're aiming at a larger architecture (separate NameNode/DataNode
  services, PostgreSQL, gRPC, a React frontend, S3-backed cloud backup,
  Kubernetes) — that's a legitimate multi-week systems project, not a
  single-file rewrite. This codebase is a good, honest starting point:
  the master-server logic, chunk placement, and re-replication rules
  here translate directly once you split nodes into real services.


## Added features in this build

- **10 GB per node:** 4 nodes = 40 GB physical demo capacity.
- **Multiple simultaneous uploads:** browser accepts multiple files and uploads them in parallel.
- **Rack-aware replica placement:** Nodes 1/2 are Rack A; Nodes 3/4 are Rack B; replicas prefer a different rack.
- **Smart placement:** projected node capacity/load, CPU/RAM/disk risk and network load influence placement.
- **Real HTTP heartbeats:** logical node agents send heartbeats every 2 seconds; the master marks a node offline after 6 seconds without one.
- **Automatic self-healing:** heartbeat failure triggers re-replication to a healthy, capacity-aware node.
- **Background integrity scanning:** every 30 seconds SHA-256 hashes are checked and a bad primary is repaired from a healthy replica.

## Added advanced deployment and operations features

- **Real independent storage-node services:** `node_service.py` exposes HTTP read/write/delete/health endpoints. In Docker mode, Node 1–4 are separate containers with their own persistent volumes and send their own heartbeats to the master.
- **PostgreSQL metadata option:** set `DATABASE_URL=postgresql://...` to use PostgreSQL instead of the default SQLite database. The Docker Compose profile includes PostgreSQL 16.
- **S3-compatible cloud backup:** set `S3_BUCKET` and AWS credentials to store hybrid-cloud backups in Amazon S3 (or an S3-compatible endpoint supported by boto3). If unset, the original local `storage/cloud_backup/` behavior remains.
- **Performance benchmark:** the new **Performance** dashboard measures live placement latency, cluster capacity, and per-node utilization. `benchmark.py` is also available for CLI measurements.
- **Containerized distributed deployment:** `docker compose up --build` starts PostgreSQL, the Flask master, and four independent storage-node services. The normal `python app.py` workflow remains unchanged and continues to use local SQLite/folder nodes.

### Docker deployment

```bash
docker compose up --build
```

Open `http://localhost:5000`.

### Optional S3 backup

Copy `.env.example` to `.env`, set `S3_BUCKET`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_REGION`, then run the Compose stack again. Leave `S3_BUCKET` empty to keep using local cloud backup.

### CLI benchmark

```bash
pip install -r requirements.txt
python benchmark.py http://127.0.0.1:5000 YOUR_USERNAME YOUR_PASSWORD
```

The benchmark reports placement latency, total/used capacity, cloud mode, and node utilization.


## Multi-file share links
The Sharing Manager now supports selecting multiple stored files and creating one secure public link. The recipient can view the collection, download individual files, or download all selected files as a ZIP. Optional password, expiry, and download limits apply to the collection link, and owners can revoke it from the dashboard.
