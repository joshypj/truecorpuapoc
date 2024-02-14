from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor


# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),  # Start date fixed to 1 days ago
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def start_job():
    print("Start ETL by Query...")

def end_job():
    print("Data insert to Table Done...")
    
# Define the DAG
dag = DAG(
    'FN00001',
    default_args=default_args,
    description='Running Stream',
    schedule_interval=None,  
    tags=['e2e example', 'ETL', 'spark'],
    params={'source_path': Param("", type="string"),
            'dest_path': Param("", type="string")},
    access_control={'All': {'can_read', 'can_edit', 'can_delete'}}
)

# Define the TriggerDagRunOperator to trigger CNTL_FRAMEWORK DAG
task1 = PythonOperator(
    task_id='Start_Data_Reading',
    python_callable=start_job,
    dag=dag,
)
task2=SparkKubernetesOperator(
    task_id='Spark_etl_submit',
    application_file="FN00001.yaml",
    do_xcom_push=True,
    dag=dag,
    api_group="sparkoperator.hpe.com",
    enable_impersonation_from_ldap_user=True
)

task3 = SparkKubernetesSensor(
    task_id='Spark_etl_monitor',
    application_name="{{ task_instance.xcom_pull(task_ids='Spark_etl_submit')['metadata']['name'] }}",
    dag=dag,
    api_group="sparkoperator.hpe.com",
    attach_log=True
)

task4 = PythonOperator(
    task_id='Data_Loading_Done',
    python_callable=end_job,
    dag=dag,
)

task1>>task2>>task3>>task4
