from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),  # Start date fixed to 2 days ago
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'FN00001',
    default_args=default_args,
    description='Running Stream',
    schedule_interval="@daily",  # Run daily at midnight
    tags=['e2e example', 'ETL', 'spark'],
    access_control={'All': {'can_read', 'can_edit', 'can_delete'}}
)

# Define the TriggerDagRunOperator to trigger CNTL_FRAMEWORK DAG
trigger_cntl_framework = TriggerDagRunOperator(
    task_id='trigger_cntl_framework',
    trigger_dag_id='CNTL_FRAMEWORK',  # specify the DAG ID of the target DAG you want to trigger
    dag=dag,
    params={'STREM_NM': 'STREM_INGESTION'}
)

# Set task dependencies
trigger_cntl_framework
