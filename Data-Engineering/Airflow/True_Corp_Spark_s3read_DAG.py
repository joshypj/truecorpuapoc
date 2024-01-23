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
    'Truecorp_spark-s3',
    default_args=default_args,
    description='Banking-data-demo',
    schedule_interval=None,
    tags=['e2e example','ETL', 'spark'],
    # params={
    #     'username': Param("hpedemo-user01", type="string"),
    #     's3_secret_name': Param("spark-s3-creds", type="string")
    # },
    access_control={
        'All': {
            'can_read',
            'can_edit',
            'can_delete'
        }
    }
)

def start_job():
    print("Start Data Reading from Hadoop s3Object store...")

def end_job():
    print("Data reading S3 Completed...")

task1 = PythonOperator(
    task_id='Start_s3Data_Reading',
    python_callable=start_job,
    dag=dag,
)
task2=SparkKubernetesOperator(
    task_id='Spark_s3read_submit',
    application_file="True_Corp_Spark_s3read.yaml",
    do_xcom_push=True,
    dag=dag,
    api_group="sparkoperator.hpe.com",
    enable_impersonation_from_ldap_user=True
)

task3 = SparkKubernetesSensor(
    task_id='Spark_s3read_monitor',
    application_name="{{ task_instance.xcom_pull(task_ids='Spark_s3read_submit')['metadata']['name'] }}",
    dag=dag,
    api_group="sparkoperator.hpe.com",
    attach_log=True
)

task4 = PythonOperator(
    task_id='Data_reading_Done',
    python_callable=end_job,
    dag=dag,
)

task1>>task2>>task3>>task4
