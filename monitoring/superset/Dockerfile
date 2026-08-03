FROM docker.arvancloud.ir/apache/superset:4.0.0

USER root
RUN pip install --no-cache-dir \
    clickhouse-connect \
    psycopg2-binary

RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

HEALTHCHECK --interval=30s --timeout=10s --retries=5 \
    CMD curl -f http://localhost:8088/health || exit 1

USER superset
EXPOSE 8088
