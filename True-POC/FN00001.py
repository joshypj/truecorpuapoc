from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),  # Start date fixed to 1 day ago
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'FN00001',
    default_args=default_args,
    description='Running Stream',
    schedule_interval='@daily',  
    tags=['e2e example', 'ETL', 'spark'],
    access_control={'All': {'can_read', 'can_edit', 'can_delete'}}
)

# Define the TriggerDagRunOperator to trigger the CNTL_FRAMEWORK DAG
trigger_cntl_framework = TriggerDagRunOperator(
    task_id='trigger_cntl_framework',
    trigger_dag_id='CNTL_FRAMEWORK',  # specify the DAG ID of the target DAG you want to trigger
    dag=dag,
    # Remove the default value and pass the parameter dynamically
    params={'STREM_NM': 'STREM_INGESTION'}  # Change the value if needed
)

# Set task dependencies
trigger_cntl_framework
