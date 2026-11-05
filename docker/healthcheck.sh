#!/usr/bin/env bash
# Container health checks, one per role. Exit 0 = healthy.
set -euo pipefail

case "${1:-}" in
  master)
    # Master UI JSON must report ALIVE (not STANDBY/RECOVERING).
    curl -fsS --max-time 5 "http://localhost:${SPARK_MASTER_WEBUI_PORT:-8080}/json/" \
      | grep -Eq '"status"[[:space:]]*:[[:space:]]*"ALIVE"'
    ;;

  worker)
    # Worker is healthy only once the master lists it as ALIVE.
    host="$(hostname)"
    curl -fsS --max-time 5 "http://${SPARK_MASTER_HOST:-spark-master}:${SPARK_MASTER_WEBUI_PORT:-8080}/json/" \
      | python -c '
import json, sys
host = sys.argv[1]
workers = json.load(sys.stdin).get("workers", [])
sys.exit(0 if any(w.get("host") == host and w.get("state") == "ALIVE" for w in workers) else 1)
' "$host"
    ;;

  connect)
    # Real HTTP/2 + gRPC handshake against the Spark Connect endpoint.
    python - <<'PY'
import os, grpc
port = os.environ.get("CONNECT_GRPC_PORT", "15002")
grpc.channel_ready_future(grpc.insecure_channel(f"localhost:{port}")).result(timeout=5)
PY
    ;;

  history)
    curl -fsS --max-time 5 -o /dev/null "http://localhost:18080/api/v1/version"
    ;;

  *)
    echo "usage: healthcheck.sh {master|worker|connect|history}" >&2
    exit 64
    ;;
esac
