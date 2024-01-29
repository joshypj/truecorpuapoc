from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago



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
    description='A DAG to demonstrate the use of BashOperator and PythonOperator in Airflow',
    schedule_interval=None,
    tags=['e2e example', 'ETL', 'spark'],
    access_control={
        'All': {
            'can_read',
            'can_edit',
            'can_delete'
        }
    }
)

# Define a Python function to print XCom data
def print_xcom_data(**kwargs):
    ti = kwargs['ti']
    ls_output = ti.xcom_pull(task_ids='run_ls_command')
    print("Output of 'ls' command:", ls_output)

# Define the BashOperator to run the ls command
run_ls_command = BashOperator(
    task_id='run_ls_command',
    bash_command='ls /mnt/user/temp',
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
