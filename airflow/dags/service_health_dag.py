from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import clickhouse_connect

default_args = {
    'owner': 'uba_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'uba_service_health_check',
    default_args=default_args,
    description='Monitors PostgreSQL and ClickHouse availability and record counts',
    schedule_interval='*/10 * * * *',
    catchup=False,
)

def check_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM users;")
    count = cursor.fetchone()[0]
    print(f"✅ Postgres Healthcheck OK! User metadata count: {count}")

def check_clickhouse_connection():
    client = clickhouse_connect.get_client(
        host='clickhouse',
        port=8123,
        username='clickhouse_admin',
        password='clickhouse_pass_2026',
        database='uba_analytics'
    )
    result = client.query("SELECT count() FROM user_events_enriched")
    count = result.result_rows[0][0]
    print(f"✅ ClickHouse Healthcheck OK! Enriched events count: {count}")

task_pg_check = PythonOperator(
    task_id='check_postgres_health',
    python_callable=check_postgres_connection,
    dag=dag,
)

task_ch_check = PythonOperator(
    task_id='check_clickhouse_health',
    python_callable=check_clickhouse_connection,
    dag=dag,
)

task_pg_check >> task_ch_check