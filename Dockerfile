# syntax=docker/dockerfile:1.7
#
# TeamSpark server image
#   Spark 4.1.4 (Scala 2.13) | Java 21 LTS (Temurin) | Python 3.12 | Ubuntu 22.04
#
# One image serves every role (master, worker, connect, history); the role is
# selected by the first argument to the entrypoint.

ARG SPARK_IMAGE=apache/spark:4.1.4-scala2.13-java21-python3-ubuntu
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.13.7

FROM ${UV_IMAGE} AS uv

FROM ${SPARK_IMAGE}

ARG PYTHON_VERSION=3.12.12

USER root

ENV TEAMSPARK_HOME=/opt/teamspark \
    UV_PYTHON_INSTALL_DIR=/opt/teamspark/python \
    UV_NO_CACHE=1 \
    UV_COMPILE_BYTECODE=1

# Pinned CPython (python-build-standalone via uv) + isolated venv.
COPY --from=uv /uv /usr/local/bin/uv
# Same lock file as the clients (setup/requirements.txt) so both sides run identical packages.
COPY setup/requirements.txt /opt/teamspark/requirements.txt
RUN uv python install "${PYTHON_VERSION}" \
 && uv venv --python "${PYTHON_VERSION}" /opt/teamspark/venv \
 && uv pip install --python /opt/teamspark/venv/bin/python -r /opt/teamspark/requirements.txt \
 && /opt/teamspark/venv/bin/python -c "import pandas, pyarrow, grpc; print('python deps ok')"

COPY --chmod=0755 docker/entrypoint.sh /opt/teamspark/bin/entrypoint.sh
COPY --chmod=0755 docker/healthcheck.sh /opt/teamspark/bin/healthcheck.sh
COPY scripts/ /opt/teamspark/scripts/

# Runtime directories (volumes/bind mounts are attached over these).
RUN mkdir -p /opt/teamspark/conf /opt/teamspark/logs /opt/teamspark/scratch /opt/teamspark/work \
             /data/warehouse /data/datasets /data/events /data/metastore /data/recovery \
 && chown -R spark:spark /opt/teamspark /data

ENV PATH=/opt/teamspark/venv/bin:${PATH} \
    SPARK_CONF_DIR=/opt/teamspark/conf \
    SPARK_LOG_DIR=/opt/teamspark/logs \
    SPARK_PID_DIR=/tmp \
    SPARK_WORKER_DIR=/opt/teamspark/work \
    PYSPARK_PYTHON=/opt/teamspark/venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/teamspark/venv/bin/python

USER spark
WORKDIR /opt/teamspark

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/teamspark/bin/entrypoint.sh"]
CMD ["help"]
