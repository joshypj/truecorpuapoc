from airflow import DAG
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 0
}

dag = DAG(
    'True_Corp_hive_data_load',
    default_args=default_args,
    schedule_interval=None,
    tags=['e2e example', 'ezaf', 'spark', 'csv', 'parquet', 'fts'],
    render_template_as_native_obj=True,
    access_control={
        'All': {
            'can_read',
            'can_edit',
            'can_delete'
        }
    }
)

test_task = BashOperator(
   task_id='test_task',
   bash_command='hostname ',
   dag=dag
)

load_data_job = SparkSubmitOperator(
    task_id='load_data_job',
    dag=dag,
    application= 'hive_dataload.py',
    env_vars={'PATH': '/bin:/usr/bin:/usr/local/bin'},
    conn_id = 'spark_con1',
    total_executor_cores='1',
    executor_cores='1',
    executor_memory='2g',
    num_executors='1',
    driver_memory='2g',
    verbose=False
)
check_data_job = SparkSubmitOperator(
    task_id='check_data_job',
    dag=dag,
    application= '/scripts/hive_dataload.py',
    conn_id = 'spark_con1',
    total_executor_cores='1',
    executor_cores='1',
    executor_memory='2g',
    num_executors='1',
    driver_memory='2g',
    verbose=False
)


test_task >> load_data_job >> check_data_job
