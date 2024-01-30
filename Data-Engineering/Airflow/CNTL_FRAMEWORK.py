# Import required modules
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Param
from airflow.utils.dates import days_ago
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
    'CNTL_FRAMEWORK',
    default_args=default_args,
    description='Running Stream',
    schedule_interval=None,
    tags=['e2e example', 'ETL', 'spark'],
    params={
        'STREM_NM': Param("X3_TEST_99_D", type="string"),
    },
    access_control={
        'All': {
            'can_read',
            'can_edit',
            'can_delete'
        }
    }
)

# Define Python functions
def start_job():
    print("Start ETL by Query...")

def end_job():
    print("Data insert to Table Done...")

# Define the tasks
task1 = PythonOperator(
    task_id='Start_Data_Reading',
    python_callable=start_job,
    dag=dag,
)

def _set_arguments(**kwargs):
    arguments_to_pass = {
        'strem_nm': kwargs['dag_run'].conf.get('STREM_NM')
    }
    return arguments_to_pass

set_arguments = PythonOperator(
    task_id='set_arguments',
    python_callable=_set_arguments,
    provide_context=True,
    dag=dag,
)

def _get_arguments(**kwargs):
    ti = kwargs['ti']
    arguments_to_pass = ti.xcom_pull(task_ids='set_arguments')
    return {'strem_nm':Param(arguments_to_pass['strem_nm'] , type="string")}

task2 = SparkKubernetesOperator(
    task_id='Spark_etl_submit',
    application_file="CNTL_FRAMEWORK.yaml",
    do_xcom_push=True,
    dag=dag,
    api_group="sparkoperator.hpe.com",
    enable_impersonation_from_ldap_user=True,
    params = _get_arguments, # Pass the function directly to arguments
)

task3 = SparkKubernetesSensor(
    task_id='Spark_etl_monitor',
    application_name="{{ ti.xcom_pull(task_ids='Spark_etl_submit')['metadata']['name'] }}",
    dag=dag,
    api_group="sparkoperator.hpe.com",
    attach_log=True
)

task4 = PythonOperator(
    task_id='Data_Loading_Done',
    python_callable=end_job,
    dag=dag,
)

# Define task dependencies
task1 >> task2 >> task3 >> task4
