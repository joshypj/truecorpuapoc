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

def submit_spark_job(**kwargs):
    strem_nm = kwargs['task_instance'].xcom_pull(task_ids='get_strem_nm')
    spark_operator_task = SparkKubernetesOperator(
        task_id='Spark_etl_submit',
        namespace='default',
        application_file="test_cntl.yaml",  # Update with your YAML file path
        do_xcom_push=True,
        arguments=["--strem_nm", strem_nm],
        dag=dag,
        pi_group="sparkoperator.hpe.com",
        enable_impersonation_from_ldap_user=True
    )
    return 'Spark job submitted successfully.'

task_submit_spark_job = PythonOperator(
    task_id='submit_spark_job',
    python_callable=submit_spark_job,
    provide_context=True,
    dag=dag,
)

# Add SparkKubernetesSensor to monitor Spark job completion
task_monitor_spark_job = SparkKubernetesSensor(
    task_id='monitor_spark_job',
    namespace='default',
    application_name="{{ task_instance.xcom_pull(task_ids='submit_spark_job') }}",
    dag=dag,
)

# Define the task dependencies
task1 >> task_submit_spark_job >> task_monitor_spark_job

