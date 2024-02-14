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
import pandas as pd 

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

def processing(**kwargs):
    strem_nm = kwargs["params"]["STREM_NM"]
    file_path = f"/mnt/shared/toh/processing/{strem_nm}.csv"
    
    df = pd.read_csv(file_path)
    for index, row in df.iterrows():
        params = row['parm']
        if row['prcs_typ'] == 1:
            source_path = params.split('^|')[0]
            dest_path = params.split('^|')[1]
            print(source_path, dest_path)
            
            # Pass parameters to the triggered DAG using TriggerDagRunOperator
            spark_task = TriggerDagRunOperator(
                task_id=f"spark_job_{row['prcs_nm']}",
                trigger_dag_id="FN00001",
                conf={'source_path': source_path, 'dest_path': dest_path},
                dag=dag
            )
            spark_task.execute(context=kwargs)

task4 = PythonOperator(
    task_id='processing',
    python_callable=processing,
    dag=dag,
)

task5 = PythonOperator(
    task_id='Data_Loading_Done',
    python_callable=end_job,
    dag=dag,
)

task1 >> task2 >> task3 >> task4 >> task5