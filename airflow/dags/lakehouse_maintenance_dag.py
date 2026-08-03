from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='uba_lakehouse_daily_maintenance',
    default_args=default_args,
    description='Daily compacting and maintenance DAG for UBA Data Platform',
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['lakehouse', 'maintenance', 'clickhouse', 'postgres'],
) as dag:

    check_postgres_health = PostgresOperator(
        task_id='check_postgres_health',
        postgres_conn_id='postgres_default',
        sql='SELECT 1;',
    )

    optimize_clickhouse_tables = BashOperator(
        task_id='optimize_clickhouse_tables',
        bash_command='curl -s "http://clickhouse:8123/?query=OPTIMIZE+TABLE+uba_analytics.user_events_enriched+FINAL;"',
    )

    log_maintenance_completion = BashOperator(
        task_id='log_maintenance_completion',
        bash_command='echo "✅ Lakehouse Daily Maintenance Finished Successfully at $(date)"',
    )

    check_postgres_health >> optimize_clickhouse_tables >> log_maintenance_completion