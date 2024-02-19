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


# List to hold groups of tasks
task_groups = []
prev_monitor_task = None
# Iterate over the DataFrame rows
for index, row in df.iterrows():
    task_id = f"task_{row['prcs_nm']}"
    
    # Create SparkKubernetesOperator for each row
    task = SparkKubernetesOperator(
        task_id=task_id,
        application_file="test_cntl.yaml",
        do_xcom_push=True,
        dag=dag,
        api_group="sparkoperator.hpe.com",
        enable_impersonation_from_ldap_user=True
    )

    # Create SparkKubernetesSensor for each row
    monitor_task = SparkKubernetesSensor(
        task_id=f"{task_id}_monitor",
        application_name=f"{{{{ task_instance.xcom_pull(task_ids='{task_id}')['metadata']['name'] }}}}",
        dag=dag,
        api_group="sparkoperator.hpe.com",
        attach_log=True
    )

    # Append the task and its monitor task to a group based on priority
    if not task_groups or task_groups[-1][-1]['prir'] != row['prir']:
        task_groups.append([])
    task_groups[-1].append({'task': task, 'monitor_task': monitor_task, 'prir': row['prir']})

    # Set up dependencies between tasks and monitor tasks
    if prev_monitor_task:
        prev_monitor_task >> task
    task >> monitor_task
    prev_monitor_task = monitor_task

# Print the tasks for verification
print(task_groups)