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
    params={'STREM_NM': Param("X3_TEST_99_D", type="string")},
    access_control={'All': {'can_read', 'can_edit', 'can_delete'}}
)


def get_strem_nm(**kwargs):
    strem_nm = kwargs["params"]["STREM_NM"]
    return strem_nm


task1 = PythonOperator(
    task_id='get_strem_nm',
    python_callable=get_strem_nm,
    provide_context=True,
    dag=dag,
)

def pass_strem_nm(**kwargs):
    return kwargs['ti'].xcom_pull(task_ids='get_strem_nm')

spark_operator_task = SparkKubernetesOperator(
    task_id='Spark_etl_submit',
    application_file="test_cntl.yaml",
    do_xcom_push=True,
    arguments=["{{ params.strem_nm }}"],
    dag=dag,
    pi_group="sparkoperator.hpe.com",
    enable_impersonation_from_ldap_user=True
)

# Set the parameter you want to pass to the Spark application
spark_operator_task.params = {
    'strem_nm': 'TEST'
}


task1 >> spark_operator_task