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


# Define a Python function to print XCom data
def print_xcom_data(**kwargs):
    ti = kwargs['ti']
    ls_output = ti.xcom_pull(task_ids='run_ls_command')
    print("Output of 'ls' command:", ls_output)

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 29),
    'retries': 1
}

# Instantiate the DAG object
dag = DAG(
    'ls_example',
    default_args=default_args,
    description='A simple DAG to run ls command and print XCom data',
    schedule_interval='@daily',
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
