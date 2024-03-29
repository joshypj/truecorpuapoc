from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Param,DagRun
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

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
    'CHILD_DAG',
    default_args=default_args,
    description='Running Stream',
    schedule_interval=None,
    tags=['e2e example', 'ETL', 'spark'],
    access_control={'All': {'can_read', 'can_edit', 'can_delete'}}
)


def get_strem_nm(**kwargs):
    strem_nm = kwargs["params"]["STREM_NM"]
    print(strem_nm)

task1=SparkKubernetesOperator(
    task_id='Spark_etl_submit',
    application_file="start_strem.yaml",
    do_xcom_push=True,
    params={"STREM_NM": 'STREM_INGESTION'},
    dag=dag,
    api_group="sparkoperator.hpe.com",
    enable_impersonation_from_ldap_user=True
)

task2 = SparkKubernetesSensor(
    task_id='Spark_etl_monitor',
    application_name="{{ task_instance.xcom_pull(task_ids='Spark_etl_submit')['metadata']['name'] }}",
    dag=dag,
    api_group="sparkoperator.hpe.com",
    attach_log=True
)
task1 >> task2