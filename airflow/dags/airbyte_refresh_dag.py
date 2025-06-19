from datetime import datetime, timedelta

from tenacity import retry
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.python import PythonSensor
from airflow.models.dagrun import DagRun
from airflow.utils.state import State
from airflow.utils.session import create_session
import logging

CONN_ID_1 = 'e914494c-affa-4571-b681-0b6fec5be478'
CONN_ID_2 = '3d578213-c0ba-462b-a144-eebfe15fe6b2'
POSTGRES_CONN_ID = 'postgres_destination'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'dag_hourly_refresh',
    default_args=default_args,
    description='Airbyte batch ingestion + DQ + DBT',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2025, 6, 17),
    catchup=False,
)

airbyte_sync_film = AirbyteTriggerSyncOperator(
    task_id='airbyte_postgres_film',
    airbyte_conn_id='airbyte',
    connection_id=CONN_ID_1,
    asynchronous=False,
    timeout=3600,
    wait_seconds=3,
    dag=dag
)

airbyte_sync_ticket = AirbyteTriggerSyncOperator(
    task_id='airbyte_postgres_ticket',
    airbyte_conn_id='airbyte',
    connection_id=CONN_ID_2,
    asynchronous=False,
    timeout=3600,
    wait_seconds=3,
    dag=dag
)

def dq_full_check():
    logger = logging.getLogger("airflow.task")
    dst = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    schema_public = 'public'
    schema_logs = 'logs'
    tables = ['users', 'films', 'film_category', 'actors', 'film_actors',
              'customers', 'cinemas', 'showtimes', 'ticket_orders']

    pk_column = {
        'users': 'id',
        'films': 'film_id',
        'film_category': 'category_id',
        'actors': 'actor_id',
        'film_actors': 'film_id, actor_id',
        'customers': 'customer_id',
        'cinemas': 'cinema_id',
        'showtimes': 'showtime_id',
        'ticket_orders': 'order_id'
    }

    for table in tables:
        logger.info(f"🔍 Checking table: {table}")

        extract_cutoff = dst.get_first(f"""
            SELECT MAX(_airbyte_extracted_at) FROM {schema_public}.{table}
        """)[0]
        logger.info(f"✅ Using extract_cutoff = {extract_cutoff}")

        # So sánh row count của batch
        public_count = dst.get_first(f"""
            SELECT COUNT(*) FROM {schema_public}.{table}
            WHERE _airbyte_extracted_at <= '{extract_cutoff}'
        """)[0]

        logs_count = dst.get_first(f"""
            SELECT COUNT(*) FROM {schema_logs}.{table}
            WHERE _airbyte_extracted_at <= '{extract_cutoff}'
        """)[0]

        assert public_count == logs_count, (
            f"❌ Row count mismatch at {extract_cutoff} in {table}: "
            f"public={public_count}, logs={logs_count}"
        )
        logger.info(f"✅ Row count match: {public_count} rows")

        if table == 'users':
            nulls = dst.get_first(f"""
                SELECT COUNT(*) FROM {schema_public}.{table}
                WHERE _airbyte_extracted_at <= '{extract_cutoff}'
                AND (email IS NULL OR first_name IS NULL OR last_name IS NULL)
            """)[0]
            assert nulls == 0, f"❌ Null values found in {table}"
            logger.info("✅ Null check passed")

        elif table == 'films':
            nulls = dst.get_first(f"""
                SELECT COUNT(*) FROM {schema_public}.{table}
                WHERE _airbyte_extracted_at <= '{extract_cutoff}'
                AND (title IS NULL OR user_rating IS NULL)
            """)[0]
            assert nulls == 0, f"❌ Nulls in {table}"
            invalid = dst.get_first(f"""
                SELECT COUNT(*) FROM {schema_public}.{table}
                WHERE _airbyte_extracted_at <= '{extract_cutoff}'
                AND (user_rating < 1 OR user_rating > 5)
            """)[0]
            assert invalid == 0, f"❌ Invalid rating"
            logger.info("✅ Domain & null check passed")

        # Duplicate PK check
        if table != 'film_actors':
            dup = dst.get_first(f"""
                SELECT COUNT(*) FROM (
                    SELECT {pk_column[table]}, COUNT(*)
                    FROM {schema_public}.{table}
                    WHERE _airbyte_extracted_at <= '{extract_cutoff}'
                    GROUP BY {pk_column[table]}
                    HAVING COUNT(*) > 1
                ) sub
            """)[0]
            assert dup == 0, f"❌ Duplicate PK in {table}"
            logger.info("✅ No duplicate PK")
        else:
            dup = dst.get_first(f"""
                SELECT COUNT(*) FROM (
                    SELECT film_id, actor_id, COUNT(*)
                    FROM {schema_public}.{table}
                    WHERE _airbyte_extracted_at <= '{extract_cutoff}'
                    GROUP BY film_id, actor_id
                    HAVING COUNT(*) > 1
                ) sub
            """)[0]
            assert dup == 0, f"❌ Duplicate composite PK"
            logger.info("✅ No duplicate composite PK")

        logger.info(f"🎯 All checks passed for batch {extract_cutoff} in {table}\n")

    logger.info("🎉🎉🎉 All tables passed DQ check for latest matching batch!")

def wait_for_next_dag_run(**context):
    dag_id = 'dag_minutely_incremental'
    dag_run_time = context['execution_date']

    with create_session() as session:  # ✅ Dùng context manager
        dagrun = (
            session.query(DagRun)
            .filter(DagRun.dag_id == dag_id)
            .filter(DagRun.execution_date > dag_run_time)
            .order_by(DagRun.execution_date.asc())
            .first()
        )

        if dagrun is None:
            context['ti'].log.info("⏳ Chưa có DAG nào sau %s", dag_run_time)
            return False

        context['ti'].log.info("🔍 Found DAG run at %s with state: %s", dagrun.execution_date, dagrun.state)
        return dagrun.state == State.SUCCESS

wait_for_next_run = PythonSensor(
    task_id='wait_next_dag_run',
    python_callable=wait_for_next_dag_run,
    mode='reschedule',
    poke_interval=30,
    timeout=120,
    dag=dag,
)

dq_task = PythonOperator(
    task_id='dq_check',
    python_callable=dq_full_check,
    dag=dag
)

dbt_run = DockerOperator(
    task_id='dbt_run',
    image='ghcr.io/dbt-labs/dbt-postgres:1.4.7',
    command=[
        "run",
        "--profiles-dir",
        "/root",
        "--project-dir",
        "/dbt",
        "--full-refresh"
    ],
    auto_remove=True,
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    mounts=[
        Mount(source='/g/Job_Project/Viettel/VDT_DE/mini_project/airbyte-airflow-data-ingestion/postgres_transformations',
              target='/dbt', type='bind'),
        Mount(source='/c/Users/ASUS/.dbt', target='/root', type='bind'),
    ],
    dag=dag
)

airbyte_sync_film >> dq_task
airbyte_sync_ticket >> dq_task
wait_for_next_run >> dq_task
dq_task >> dbt_run