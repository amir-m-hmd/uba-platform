FROM apache/airflow:2.10.2-python3.11

USER 0

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER 50000

RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres==5.11.1 \
    apache-airflow-providers-apache-spark==4.8.1 \
    apache-airflow-providers-common-sql==1.14.0 \
    psycopg2-binary==2.9.9