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
import pandas as pd
import os

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
    'CNTL_FRAMEWORK',
    default_args=default_args,
    description='Running Stream',
    schedule_interval=None,
    tags=['e2e example', 'ETL', 'spark'],
    params={'STREM_NM': Param("STREM_INGESTION", type="string")},
    access_control={'All': {'can_read', 'can_edit', 'can_delete'}}
)

def start_job(**kwargs):
    print("Start ETL by Query...")
    strem_nm = kwargs['dag_run'].conf.get('STREM_NM')
    print(f"Parameter STREM_NM: {strem_nm}")

def end_job():
    print("Data insert to Table Done...")

# Define the tasks
task1 = PythonOperator(
    task_id='Start_Data_Reading',
    python_callable=start_job,
    provide_context=True,  # This is important to access the dag_run object
    dag=dag,
)

task2 = SparkKubernetesOperator(
    task_id='Spark_etl_submit',
    application_file="CNTL_FRAMEWORK.yaml",
    do_xcom_push=True,
    api_group="sparkoperator.hpe.com",
    enable_impersonation_from_ldap_user=True,
    dag=dag,
)

task3 = SparkKubernetesSensor(
    task_id='Spark_etl_monitor',
    application_name="{{ ti.xcom_pull(task_ids='Spark_etl_submit')['metadata']['name'] }}",
    dag=dag,
    api_group="sparkoperator.hpe.com",
    attach_log=True,
    do_xcom_push=True,
)

task4 = PythonOperator(
    task_id='Data_Loading_Done',
    python_callable=end_job,
    dag=dag,
)

# Set task dependencies
task1 >> task2 >> task3 >> task4