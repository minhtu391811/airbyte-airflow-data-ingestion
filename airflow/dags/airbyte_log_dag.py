from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

CONN_ID_1 = '13cf0099-a530-4ae3-89c7-7339ebf42fa9'
CONN_ID_2 = 'baaa7b2c-ed81-4f88-856f-2a147f749b5e'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
}

dag = DAG(
    'dag_minutely_incremental',
    default_args=default_args,
    description='Airbyte sync logs',
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2025, 6, 17),
    catchup=False,
)

airbyte_log_film = AirbyteTriggerSyncOperator(
    task_id='airbyte_logs_film',
    airbyte_conn_id='airbyte',
    connection_id=CONN_ID_1,
    asynchronous=False,
    timeout=600,
    wait_seconds=2,
    dag=dag
)

airbyte_log_ticket = AirbyteTriggerSyncOperator(
    task_id='airbyte_logs_ticket',
    airbyte_conn_id='airbyte',
    connection_id=CONN_ID_2,
    asynchronous=False,
    timeout=600,
    wait_seconds=2,
    dag=dag
)