# TeamSpark Server

Centralized Apache Spark environment hosted on one Mac with Docker Compose. Team
members connect remotely with PySpark over **Spark Connect** through Tailscale.

| Component | Version |
|---|---|
| Apache Spark (Scala 2.13, Hadoop 3.4.2) | 4.1.4 |
| Java (Temurin LTS) | 21 |
| Python (server + executors) | 3.12.12 |
| Clients must use | Python 3.12 + `pyspark-client==4.1.4` |

## Architecture

```
            Tailscale (100.x.y.z:15002)                    Mac-local only (127.0.0.1)
                       │                                 :8080 master UI  :4040 driver UI  :18080 history
                       ▼
 ┌──────────────── teamspark-net (private Docker bridge) ────────────────────────┐
 │  spark-connect  ── driver of one shared Spark app (FAIR scheduler)            │
 │       │ 7078/7079 (internal)                                                  │
 │  spark-master :7077 (internal) ◄── spark-worker-1, spark-worker-2             │
 │  spark-history  (reads ./storage/events)                                      │
 └───────────────────────────────────────────────────────────────────────────────┘
```

* **Only port 15002 (Spark Connect)** is meant for users. The master RPC (7077), driver/block-manager
  ports (7078/7079) and worker UIs (8081) are never published. The admin UIs are bound to `127.0.0.1`.
* **Persistent storage:**
  * `./storage/warehouse` holds managed tables.
  * `./storage/datasets` holds input files, visible at `/data/datasets` to the driver and executors.
  * `./storage/events` holds event logs.
  * Docker volumes hold the Hive (Derby) metastore, the master recovery state, and scratch space.
* **Resources:** all sizing lives in `.env`. The defaults fit Docker Desktop with about 8 GB RAM. `.env.example` also lists a 12 GB profile.

## Files

| Path | Purpose |
|---|---|
| `Dockerfile` | One image for all roles: `apache/spark:4.1.4` + pinned CPython 3.12 venv |
| `docker-compose.yml` | Master, 2 workers, Connect server, History server, network, health checks, limits |
| `.env` / `.env.example` | Versions, ports, bind IP, CPU/memory sizing |
| `conf/spark-defaults.conf` | Spark config, mounted read-only into every container |
| `requirements.txt` | Pinned server-side Python packages |
| `docker/entrypoint.sh` | Role launcher (`master`, `worker`, `connect`, `history`) |
| `docker/healthcheck.sh` | Per-role health checks. A worker is healthy only when it is registered ALIVE; Connect is checked with a real gRPC handshake |
| `scripts/smoke_test.py` | End-to-end verification |

## Start

Prerequisites: Docker Desktop. Set its memory to at least 8 GB under Settings → Resources.

```bash
cd ~/Projects/teamspark
cp -n .env.example .env              # first time only; edit sizing if needed
docker compose build                 # first build ~5 min
docker compose up -d --wait          # returns when every service is healthy (~40 s)
docker compose ps
```

## Verify

```bash
docker compose exec spark-connect python /opt/teamspark/scripts/smoke_test.py
```

Expected: `ALL CHECKS PASSED`. The test checks:

* the Spark version;
* a distributed aggregation;
* a shuffle join;
* a Python UDF that runs on **both** workers;
* an Arrow/pandas round trip;
* a persistent Hive table.

Useful UIs on the Mac only:

* Master: http://localhost:8080 (worker and app UIs are reverse-proxied through it)
* Live driver: http://localhost:4040
* History: http://localhost:18080

## Operate

```bash
docker compose logs -f spark-connect        # follow logs
docker compose restart spark-connect        # after editing conf/spark-defaults.conf
docker compose up -d                        # after editing .env
docker compose build && docker compose up -d   # after changing Dockerfile/requirements
docker compose down                         # stop (data kept)
docker compose down -v                      # stop AND delete metastore/recovery volumes
```

**Add a worker:**

1. Copy the `spark-worker-2` block in `docker-compose.yml` as `spark-worker-3`.
2. Rename its hostname, container name and volumes, and declare the new volumes.
3. Add it to `spark-connect.depends_on`.
4. Raise `SPARK_CORES_MAX` in `.env`.

## Prepare for Tailscale

Spark Connect has **no authentication** in this setup. Tailscale and its ACLs are the access
control, so never publish 15002 on `0.0.0.0` or the LAN.

1. Install Tailscale on the Mac and log in: `brew install --cask tailscale`, or use the App Store app.
2. Expose Connect to the tailnet with **one** of these options.

   **Option A (recommended): Tailscale forwards to localhost.** Keep `CONNECT_BIND_IP=127.0.0.1`. Then run:
   ```bash
   tailscale serve --bg --tcp 15002 tcp://127.0.0.1:15002
   tailscale serve status
   ```
   Docker never binds to the Tailscale interface, so the stack still starts if Tailscale is down or restarting.

   **Option B: bind Docker to the Tailscale IP.** Set `CONNECT_BIND_IP=$(tailscale ip -4)` in `.env`, then run
   `docker compose up -d`. Tailscale must be connected before the stack starts, or the port bind fails.

3. Restrict access in the Tailscale admin console ACLs. For example:
   ```json
   { "grants": [ { "src": ["group:teamspark"], "dst": ["tag:teamspark-server"], "ip": ["tcp:15002"] } ] }
   ```
4. Clients connect with `sc://<mac-tailscale-name-or-100.x.y.z>:15002`. To set up a client, follow
   [setup/README.md](setup/README.md): run `setup/setup.ps1` on Windows or `setup/setup.sh` on macOS.

## Notes

* All users share **one** Spark application, the Connect server. The FAIR scheduler stops one user's
  job from starving the others. Temp views and session settings stay per-session; tables are shared.
* Python UDFs run on the executors' Python 3.12, so clients need Python 3.12 for UDFs and pandas UDFs.
* The Derby metastore is single-writer. That is fine because only the Connect server uses it.
