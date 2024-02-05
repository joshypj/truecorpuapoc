from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models.param import Param
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
dag = DAG(
    'MysqlDB_Transform',
    default_args=default_args,
    description='Banking-data-demo',
    schedule_interval=None,
    tags=['e2e example','ETL', 'spark'],
    access_control={
        'All': {
            'can_read',
            'can_edit',
            'can_delete'
        }
    }
)

def start_job():
    print("Starting the Data reading from Raw Zone.......")

def end_job():
    print("Data Transformed and written to Staging zone.......... ")

task1 = PythonOperator(
    task_id='Start_Data_Reading',
    python_callable=start_job,
    dag=dag,
)
task2=SparkKubernetesOperator(
    task_id='MysqlDB_Transform_to_Delta_Lake',
    application_file="MysqlDB_Transform.yaml",
    do_xcom_push=True,
    dag=dag,
    api_group="sparkoperator.hpe.com",
    enable_impersonation_from_ldap_user=True
)

task3 = SparkKubernetesSensor(
    task_id='MysqlDB_Batch_Job_Monitor',
    application_name="{{ task_instance.xcom_pull(task_ids='MysqlDB_Transform_to_Delta_Lake')['metadata']['name'] }}",
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
