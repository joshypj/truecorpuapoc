from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.dates import days_ago
import pandas as pd

# Create or read your DataFrame
data = {
    "prcs_nm": ["ABC_1", "ABC_2", "ABC_3", "ABC_4"],
    "strem_nm": ["STREM_ABC"] * 4,
    "prir": [1, 2, 2, 3]
}
df = pd.DataFrame(data)

# Define default_args for your DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define your DAG
dag = DAG(
    'dynamic_spark_tasks_creator',
    default_args=default_args,
    description='Create dynamic Spark tasks',
    schedule_interval=None,
    tags=['spark', 'kubernetes']
)

# Dictionary to hold references to the tasks
tasks = {}

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

    # Add the task to the tasks dictionary
    tasks[task_id] = task

    # Create SparkKubernetesSensor for each row
    monitor_task = SparkKubernetesSensor(
        task_id=f"{task_id}_monitor",
        application_name=f"{{{{ task_instance.xcom_pull(task_ids='{task_id}')['metadata']['name'] }}}}",
        dag=dag,
        api_group="sparkoperator.hpe.com",
        attach_log=True
    )

    # Set up task dependencies
    if row['prir'] > 1:
        monitor_task.set_upstream(tasks[f"task_{df.iloc[index - 1]['prcs_nm']}"])

# Set the downstream task
for task_id, task in tasks.items():
    if task_id != 'task_ABC_1':
        task.set_downstream(tasks[task_id])

# Print the tasks for verification
print(tasks)
