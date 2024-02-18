from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Param,DagRun
from airflow.operators.python_operator import PythonOperator

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
    'PARENT_DAG',
    default_args=default_args,
    description='Running Stream',
    schedule_interval=None,
    tags=['e2e example', 'ETL', 'spark'],
    params={'STREM_NM': Param("X3_TEST_99_D", type="string")},
    access_control={'All': {'can_read', 'can_edit', 'can_delete'}}
)

def get_strem_nm(**kwargs):
    strem_nm = kwargs["params"]["STREM_NM"]
    trigger_task = TriggerDagRunOperator(
                    task_id="trigger_task",
                    trigger_dag_id="CHILD_DAG",
                    conf={'STREM_NM': strem_nm},
                    dag=dag
                )

task1 = PythonOperator(
    task_id='get_strem_nm',
    python_callable=get_strem_nm,
    provide_context=True,
    dag=dag,
)

task1