from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
import pandas as pd

# Create or read your DataFrame
data = {
    "prcs_nm": ["ABC_1", "ABC_2", "ABC_3", "ABC_4"],
    "strem_nm": ["STREM_ABC"] * 4,
    "prir": [1, 2, 2, 3]
}
df = pd.DataFrame(data)

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

# Dictionary to hold references to the tasks
tasks = {}

# Iterate over the DataFrame rows
last_task = None
for index, row in df.iterrows():
    task_id = f"task_{row['prcs_nm']}"
    monitor_task_id = f"{task_id}_monitor"  # Corrected monitor_task_id generation

    # Create SparkKubernetesOperator for each row
    task = SparkKubernetesOperator(
        task_id=task_id,
        application_file="test_cntl.yaml",
        do_xcom_push=True,
        dag=dag,
        api_group="sparkoperator.hpe.com",
        enable_impersonation_from_ldap_user=True
    )

    # Add the task to the tasks dictionary
    tasks[task_id] = task

    # Create SparkKubernetesSensor for each row
    monitor_task = SparkKubernetesSensor(
        task_id=monitor_task_id,
        application_name=f"{{{{ task_instance.xcom_pull(task_ids='{task_id}')['metadata']['name'] }}}}",
        dag=dag,
        api_group="sparkoperator.hpe.com",
        attach_log=True
    )

    if last_task:
        if row['prir'] > df.iloc[index - 1]['prir']:
            # If the current task has a higher priority than the previous one,
            # it starts a new group
            last_task.set_downstream(task)
        else:
            # If the current task has the same priority as the previous one,
            # it belongs to the same group
            last_task.set_downstream(task)
            tasks[last_monitor_task_id].set_upstream(tasks[task_id])  # Corrected reference to the last monitor task
    else:
        # For the first task
        last_task = task

    last_monitor_task_id = monitor_task_id  # Update the last monitor task ID

# Print the tasks for verification
print(tasks)