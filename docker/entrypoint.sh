#!/usr/bin/env bash
# TeamSpark role entrypoint: master | worker | connect | history
# Every daemon runs in the foreground so tini/Docker can supervise it and
# logs go to stdout (docker compose logs).
set -euo pipefail

: "${SPARK_HOME:=/opt/spark}"
: "${SPARK_MASTER_HOST:=spark-master}"
: "${SPARK_MASTER_PORT:=7077}"
MASTER_URL="spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}"

role="${1:-help}"
shift || true

case "$role" in
  master)
    exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.master.Master \
      --host "${SPARK_MASTER_HOST}" \
      --port "${SPARK_MASTER_PORT}" \
      --webui-port "${SPARK_MASTER_WEBUI_PORT:-8080}" "$@"
    ;;

  worker)
    exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.worker.Worker \
      --cores  "${SPARK_WORKER_CORES:?SPARK_WORKER_CORES is required}" \
      --memory "${SPARK_WORKER_MEMORY:?SPARK_WORKER_MEMORY is required}" \
      --work-dir "${SPARK_WORKER_DIR}" \
      --webui-port "${SPARK_WORKER_WEBUI_PORT:-8081}" \
      "$@" "$MASTER_URL"
    ;;

  connect)
    # --wait keeps the Spark Connect server in the foreground.
    exec "$SPARK_HOME/sbin/start-connect-server.sh" --wait \
      --master "$MASTER_URL" \
      --name "${CONNECT_APP_NAME:-TeamSpark Connect}" \
      --driver-memory  "${CONNECT_DRIVER_MEMORY:?CONNECT_DRIVER_MEMORY is required}" \
      --executor-memory "${SPARK_EXECUTOR_MEMORY:?SPARK_EXECUTOR_MEMORY is required}" \
      --conf "spark.executor.cores=${SPARK_EXECUTOR_CORES:?SPARK_EXECUTOR_CORES is required}" \
      --conf "spark.cores.max=${SPARK_CORES_MAX:?SPARK_CORES_MAX is required}" \
      --conf "spark.connect.grpc.binding.port=${CONNECT_GRPC_PORT:-15002}" \
      "$@"
    ;;

  history)
    exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.history.HistoryServer "$@"
    ;;

  help|*)
    echo "usage: entrypoint.sh {master|worker|connect|history} [extra spark args]" >&2
    [ "$role" = help ] && exit 0 || exit 64
    ;;
esac
