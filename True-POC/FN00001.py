from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Param,DagRun
from airflow.utils.dates import days_ago
from airflow.utils.helpers import chain
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.operators.dummy import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.combine(datetime.today(), datetime.min.time()) + timedelta(hours=17),  # Start today at 17:00
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'params': {'STREM_NM': Param("STREM_INGESTION", type="string")},
    'access_control': {'All': {'can_read', 'can_edit', 'can_delete'}}
}

# Define the DAG
dag = DAG(
    'FN00001',
    default_args=default_args,
    description='Running Stream',
    schedule_interval="*/30 * * * *",  # Run every 30 minutes
    tags=['e2e example', 'ETL', 'spark']
)

# Define the TriggerDagRunOperator to trigger CNTL_FRAMEWORK DAG
trigger_cntl_framework = TriggerDagRunOperator(
    task_id='trigger_cntl_framework',
    trigger_dag_id='CNTL_FRAMEWORK',  # specify the DAG ID of the target DAG you want to trigger
    dag=dag,
)

# Define the ExternalTaskSensor to monitor the CNTL_FRAMEWORK DAG
monitor_cntl_framework = ExternalTaskSensor(
    task_id='monitor_cntl_framework',
    external_dag_id='CNTL_FRAMEWORK',  # specify the DAG ID of the target DAG
    external_task_id='task_id_to_monitor',  # specify the task ID within the target DAG
    mode='reschedule',  # reschedule the sensor task
    poke_interval=60,  # check every 60 seconds
    timeout=7200,  # timeout after 2 hours (adjust as needed)
    dag=dag,
)

# Set task dependencies
trigger_cntl_framework >> monitor_cntl_framework