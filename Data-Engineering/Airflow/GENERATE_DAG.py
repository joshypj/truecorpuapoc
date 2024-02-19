from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

def generate_dynamic_dag(configs):
    for config_name, config in configs.items():
        dag_id = f"dynamic_generated_dag_{config_name}"

        dag = DAG(
            dag_id=dag_id,
            start_date=datetime(2022, 2, 1),
            schedule_interval=None,  # You may set the schedule interval as per your requirement
            access_control={
                'All': {
                    'can_read',
                    'can_edit',
                    'can_delete'
                }
            }
        )

        def print_message(message):
            print(message)

        with dag:
            print_message_task = PythonOperator(
                task_id=f"print_message_task_{config_name}",
                python_callable=print_message,
                op_kwargs={"message": config["message"]}
            )

        print_message_task

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'dynamic_dag_creator',
    default_args=default_args,
    description='Create dynamic DAGs',
    schedule_interval=None,  # You may set the schedule interval as per your requirement
    tags=['e2e example','ETL', 'spark'],
    access_control={
        'All': {
            'can_read',
            'can_edit',
            'can_delete'
        }
    }
)

configs = {
    "config1": {"message": "first DAG will receive this message"},
    "config2": {"message": "second DAG will receive this message"},
}

create_dags_task = PythonOperator(
    task_id='create_dags_task',
    python_callable=generate_dynamic_dag,
    op_kwargs={'configs': configs},
    dag=dag
)

create_dags_task
