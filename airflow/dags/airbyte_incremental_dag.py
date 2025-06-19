from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

CONN_ID_1 = 'a97230c7-9251-4725-af06-7fe70c2faa71'
CONN_ID_2 = '1a1ea582-aa7c-4d15-b065-95b5f942102d'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
}

dag = DAG(
    'dag_minutely_incremental',
    default_args=default_args,
    description='Airbyte sync incremental every minute',
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2025, 6, 17),
    catchup=False,
)

airbyte_incremental_film = AirbyteTriggerSyncOperator(
    task_id='airbyte_incremental_film',
    airbyte_conn_id='airbyte',
    connection_id=CONN_ID_1,
    asynchronous=False,
    timeout=600,
    wait_seconds=2,
    dag=dag
)

airbyte_incremental_ticket = AirbyteTriggerSyncOperator(
    task_id='airbyte_incremental_ticket',
    airbyte_conn_id='airbyte',
    connection_id=CONN_ID_2,
    asynchronous=False,
    timeout=600,
    wait_seconds=2,
    dag=dag
)