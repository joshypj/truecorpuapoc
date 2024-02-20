from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os 
import pandas as pd

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

# Define a Python function to read a text file and print its content
def read_and_print_file(**kwargs):
    ti = kwargs['ti']
    file_path = '/mnt/shared/Toh/processing/STREM_ABC.csv'  # Change this to your file path
    print(file_path)
    df = pd.read_csv(file_path)
    print(df)

# Define the PythonOperator to read the text file and print its content
read_file_task = PythonOperator(
    task_id='read_file',
    python_callable=read_and_print_file,
    provide_context=True,
    dag=dag,
)

read_file_task