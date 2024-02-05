from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Param,DagRun
from airflow.utils.dates import days_ago
from airflow.utils.helpers import chain
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.operators.dummy import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

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
    params={'STREM_NM': Param("X3_TEST_99_D", type="string")},
    access_control={'All': {'can_read', 'can_edit', 'can_delete'}}
)

# Define Python functions
def start_job():
    print("Start ETL by Query...")

def end_job():
    print("Data insert to Table Done...")

def read_and_print_file(**kwargs):
    ti = kwargs['ti']
    file_path = '/mnt/shared/Toh/Queue.txt'  # Change this to your file path
    try:
        with open(file_path, 'r') as file:
            file_content = file.read()
            print("Content of the file:")
            print(file_content)
            ti.xcom_push(key='file_content', value=file_content)
    except FileNotFoundError:
        print(f"File not found at {file_path}")




# Define the tasks
task1 = PythonOperator(
    task_id='Start_Data_Reading',
    python_callable=start_job,
    dag=dag,
)

task2 = SparkKubernetesOperator(
    task_id='Spark_etl_submit',
    application_file="CNTL_FRAMEWORK.yaml",
    do_xcom_push=True,
    api_group="sparkoperator.hpe.com",
    enable_impersonation_from_ldap_user=True,
    dag=dag,
)

task3 = SparkKubernetesSensor(
    task_id='Spark_etl_monitor',
    application_name="{{ ti.xcom_pull(task_ids='Spark_etl_submit')['metadata']['name'] }}",
    dag=dag,
    api_group="sparkoperator.hpe.com",
    attach_log=True,
    do_xcom_push=True,
)

read_file_task = PythonOperator(
    task_id='read_file',
    python_callable=read_and_print_file,
    provide_context=True,
    dag=dag,
)

def condition(**kwargs):
    parm = kwargs['ti'].xcom_pull(task_ids='read_file', key='file_content')
    type = parm.split('^|')[-1]

    if type == '1':
        return 'taskA'
    else:
        return 'taskB'
branching_task = BranchPythonOperator(
    task_id='branching_task',
    python_callable=condition,
    dag=dag,
)

parameter_value = "{{ ti.xcom_pull(task_ids='read_file', key='file_content') }}"
print('parameter_value :',parameter_value)
taskA = TriggerDagRunOperator(
    task_id='taskA',
    trigger_dag_id="TEST_CNTL_1",
    do_xcom_push=True,
    dag=dag,
    conf={'parm': parameter_value}  # Pass parameters to the triggered DAG run
)


taskB = DummyOperator(
    task_id='taskB',
    dag=dag,
)

def check_triggered_dag_status(**kwargs):
    # Retrieve the triggered DAG run
    dag_run_id = kwargs['ti'].xcom_pull(task_ids='Trigger_dag')
    dag_run = DagRun.find(dag_id="TEST_CNTL_1", run_id=dag_run_id)
    
    # Check if the DAG run exists and print its status
    if dag_run:
        print("Status of TEST_CNTL_1:", dag_run[0].state)
    else:
        print("No record found for TEST_CNTL_1")

taskAmonitor = PythonOperator(
    task_id='taskA_monitor',
    python_callable=check_triggered_dag_status,
    provide_context=True,
    dag=dag,
)

taskBmonitor = DummyOperator(
    task_id='taskB_monitor',
    dag=dag,
)


insert_log = SparkKubernetesOperator(
    task_id='insert_log',
    application_file="insert_log.yaml",
    do_xcom_push=True,
    api_group="sparkoperator.hpe.com",
    enable_impersonation_from_ldap_user=True,
    dag=dag,
)

monitor_insert_log = SparkKubernetesSensor(
    task_id='monitor_insert_log',
    application_name="{{ ti.xcom_pull(task_ids='insert_log')['metadata']['name'] }}",
    dag=dag,
    api_group="sparkoperator.hpe.com",
    attach_log=True,
    do_xcom_push=True,
)

task4 = PythonOperator(
    task_id='Data_Loading_Done',
    python_callable=end_job,
    dag=dag,
)

# Define task dependencies
task1 >> task2 >> task3 >> read_file_task >> branching_task

branching_task >> taskA
taskA >> taskAmonitor
taskAmonitor >> insert_log

branching_task >> taskB
taskB >> taskBmonitor
taskBmonitor >> insert_log

insert_log >> task4

