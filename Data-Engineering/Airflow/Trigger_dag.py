from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Param

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'Trigger_dag',
    default_args=default_args,
    description='Trigger CNTL_FRAMEWORK',
    schedule_interval=None,
    tags=['e2e example', 'CNTL', 'Trigger'],
    params={'STREM_NM': Param("TEST", type="string")},
    access_control={'All': {'can_read', 'can_edit', 'can_delete'}}
)

def start_job():
    print("Start ETL by Query...")

def end_job():
    print("Data insert to Table Done...")

# Define the TriggerDagRunOperator
def trigger_dag(**kwargs):
    strem_nm = kwargs['params']['STREM_NM']
    return TriggerDagRunOperator(
        task_id='call_CNTL_FRAMEWORK',
        trigger_dag_id='CNTL_FRAMEWORK',
        conf={'STREM_NM': strem_nm},
        dag=dag
)

# Define tasks
start_job = PythonOperator(
    task_id='start_job',
    python_callable=start_job,
    dag=dag
)

call_trigger_dag = PythonOperator(
    task_id='call_trigger_dag',
    python_callable=trigger_dag,
    dag=dag
)


end_job = PythonOperator(
    task_id='end_job',
    python_callable=end_job,
    dag=dag
)

# Set task dependencies
start_job >> call_trigger_dag >> end_job
