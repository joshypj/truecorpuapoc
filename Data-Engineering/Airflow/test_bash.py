from airflow import DAG
from datetime import datetime
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models.param import Param
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 29),
    'retries': 1
}

dag = DAG(
    'ls_example',
    default_args=default_args,
    description='A simple DAG to run ls command',
    schedule_interval=None,
)

# สร้าง BashOperator เพื่อรันคำสั่ง ls
ls_task = BashOperator(
    task_id='run_ls_command',
    bash_command='ls',
    dag=dag,
)

ls_task