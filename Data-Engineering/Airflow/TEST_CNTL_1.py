from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models.param import Param
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor




default_args = {
    'owner': 'airflow_CNTL',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': 'None',
}
dag = DAG(
    'TEST_CNTL_1',
    default_args=default_args,
    schedule_interval=None,
    tags=['cntl example','test', 'spark'],
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
    print("Start Job")

def end_job():
    print("End Job")

task1 = PythonOperator(
    task_id='start',
    python_callable=start_job,
    dag=dag,
)


task2 = PythonOperator(
    task_id='end',
    python_callable=end_job,
    dag=dag,
)

task1>>task2
