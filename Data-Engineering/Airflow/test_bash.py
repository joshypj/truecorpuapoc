from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models.param import Param
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

# Define a Python function to print XCom data
def print_xcom_data(**kwargs):
    ti = kwargs['ti']
    ls_output = ti.xcom_pull(task_ids='run_ls_command')
    print("Output of 'ls' command:", ls_output)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
dag = DAG(
    'test_bash',
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


# Define the BashOperator to run the ls command and push the result to XCom
run_ls_command = BashOperator(
    task_id='run_ls_command',
    bash_command='ls',
    xcom_push=True,  # Push the output of the command to XCom
    dag=dag,
)

# Define the PythonOperator to print XCom data
print_xcom = PythonOperator(
    task_id='print_xcom_data',
    python_callable=print_xcom_data,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
run_ls_command >> print_xcom