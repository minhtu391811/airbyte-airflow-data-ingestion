from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
import logging

CONN_ID_1 = 'a97230c7-9251-4725-af06-7fe70c2faa71'
CONN_ID_2 = '1a1ea582-aa7c-4d15-b065-95b5f942102d'
SRC_CONN_ID_1 = 'film_postgres'
SRC_CONN_ID_2 = 'ticket_postgres'
DST_CONN_ID = 'destination_postgres'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
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

    # Source connections
    src_1 = PostgresHook(postgres_conn_id=SRC_CONN_ID_1)
    src_2 = PostgresHook(postgres_conn_id=SRC_CONN_ID_2)
    dst = PostgresHook(postgres_conn_id=DST_CONN_ID)

    # film DB tables
    film_tables = ['users', 'films', 'film_category', 'actors', 'film_actors']
    # Ticket DB tables
    ticket_tables = ['customers', 'cinemas', 'showtimes', 'ticket_orders']

    # Mapping table to source
    table_source_map = {table: src_1 for table in film_tables}
    table_source_map.update({table: src_2 for table in ticket_tables})

    # Mapping primary keys
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

    all_tables = film_tables + ticket_tables

    for table in all_tables:
        logger.info(f"🔎 Starting data quality check for table: {table}")
        src = table_source_map[table]

        # Row count check
        src_count = src.get_first(f"SELECT COUNT(*) FROM {table}")[0]
        dst_count = dst.get_first(f"SELECT COUNT(*) FROM {table}")[0]
        assert src_count == dst_count, f"❌ Row count mismatch in {table}: src={src_count}, dst={dst_count}"
        logger.info(f"✅ Row count check passed for {table} (count = {src_count})")

        # Null checks
        if table == 'users':
            nulls = dst.get_first(f"""
                SELECT COUNT(*) FROM {table} 
                WHERE email IS NULL OR first_name IS NULL OR last_name IS NULL
            """)[0]
            assert nulls == 0, f"❌ Null values found in {table}"
            logger.info(f"✅ Null check passed for {table}")

        elif table == 'films':
            nulls = dst.get_first(f"""
                SELECT COUNT(*) FROM {table} 
                WHERE title IS NULL OR user_rating IS NULL
            """)[0]
            assert nulls == 0, f"❌ Null values found in {table}"
            logger.info(f"✅ Null check passed for {table}")
            invalid_rating = dst.get_first(f"""
                SELECT COUNT(*) FROM {table} 
                WHERE user_rating < 1 OR user_rating > 5
            """)[0]
            assert invalid_rating == 0, f"❌ Invalid user_rating in {table}"
            logger.info(f"✅ Domain check on user_rating passed for {table}")

        elif table == 'film_category':
            nulls = dst.get_first(f"""
                SELECT COUNT(*) FROM {table} 
                WHERE category_name IS NULL
            """)[0]
            assert nulls == 0, f"❌ Null values found in {table}"
            logger.info(f"✅ Null check passed for {table}")

        elif table == 'actors':
            nulls = dst.get_first(f"""
                SELECT COUNT(*) FROM {table} 
                WHERE actor_name IS NULL
            """)[0]
            assert nulls == 0, f"❌ Null values found in {table}"
            logger.info(f"✅ Null check passed for {table}")

        elif table == 'customers':
            nulls = dst.get_first(f"""
                SELECT COUNT(*) FROM {table} 
                WHERE full_name IS NULL OR email IS NULL
            """)[0]
            assert nulls == 0, f"❌ Null values in {table}"
            logger.info(f"✅ Null check passed for {table}")

        elif table == 'cinemas':
            nulls = dst.get_first(f"""
                SELECT COUNT(*) FROM {table} 
                WHERE name IS NULL OR location IS NULL
            """)[0]
            assert nulls == 0, f"❌ Null values in {table}"
            logger.info(f"✅ Null check passed for {table}")

        elif table == 'showtimes':
            nulls = dst.get_first(f"""
                SELECT COUNT(*) FROM {table} 
                WHERE film_id IS NULL OR cinema_id IS NULL OR show_time IS NULL
            """)[0]
            assert nulls == 0, f"❌ Null values in {table}"
            logger.info(f"✅ Null check passed for {table}")

        elif table == 'ticket_orders':
            nulls = dst.get_first(f"""
                SELECT COUNT(*) FROM {table} 
                WHERE customer_id IS NULL OR showtime_id IS NULL OR total_price IS NULL OR payment_status IS NULL
            """)[0]
            assert nulls == 0, f"❌ Null values in {table}"
            logger.info(f"✅ Null check passed for {table}")

        # Duplicate PK check
        if table != 'film_actors':
            dup = dst.get_first(f"""
                SELECT COUNT(*) FROM (
                    SELECT {pk_column[table]}, COUNT(*) 
                    FROM {table} 
                    GROUP BY {pk_column[table]} 
                    HAVING COUNT(*) > 1
                ) sub
            """)[0]
            assert dup == 0, f"❌ Duplicate PK found in {table}"
            logger.info(f"✅ Duplicate PK check passed for {table}")
        else:
            dup = dst.get_first(f"""
                SELECT COUNT(*) FROM (
                    SELECT film_id, actor_id, COUNT(*) 
                    FROM {table} 
                    GROUP BY film_id, actor_id 
                    HAVING COUNT(*) > 1
                ) sub
            """)[0]
            assert dup == 0, f"❌ Duplicate composite PK in {table}"
            logger.info(f"✅ Duplicate composite PK check passed for {table}")

        logger.info(f"🎯 All data quality checks PASSED for table: {table}\n")

    logger.info("🎉🎉🎉 Data quality check COMPLETED SUCCESSFULLY for ALL tables!")

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

airbyte_sync_film >> dq_task >> dbt_run
airbyte_sync_ticket >> dq_task >> dbt_run
