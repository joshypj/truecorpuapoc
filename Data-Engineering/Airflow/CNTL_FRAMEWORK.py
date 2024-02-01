from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
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

def condition(**kwargs):
    parm = kwargs['ti'].xcom_pull(task_ids='read_file', key='file_content')
    type = parm.split('^|')[-1]

    if type == '1':
        return 'taskA'
    else:
        return 'taskB'

def submit_spark_etl(**kwargs):
    parameter_value = kwargs['ti'].xcom_pull(task_ids='read_file')
    spark_operator = SparkKubernetesOperator(
        task_id='Spark_etl_submit',
        application_file="test_cntl.yaml",
        do_xcom_push=True,
        api_group="sparkoperator.hpe.com",
        enable_impersonation_from_ldap_user=True,
        dag=dag,
        parameters={"parm": parameter_value.split('^|')[1]}
    )
    return 'Spark_etl_submit'

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

branching_task = BranchPythonOperator(
    task_id='branching_task',
    python_callable=condition,
    dag=dag,
)

taskA = PythonOperator(
    task_id='taskA',
    python_callable=submit_spark_etl,
    provide_context=True,
    dag=dag,
)

taskB = DummyOperator(
    task_id='taskB',
    dag=dag,
)

taskAmonitor = SparkKubernetesSensor(
    task_id='taskA_monitor',
    application_name="{{ ti.xcom_pull(task_ids='Spark_etl_submit')['metadata']['name'] }}",
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
branching_task >> taskB
taskA >> taskAmonitor >> task4
taskB >> task4
