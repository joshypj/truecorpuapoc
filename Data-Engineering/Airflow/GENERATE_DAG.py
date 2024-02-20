from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
import pandas as pd

# Create or read your DataFrame
data = {
    "prcs_nm": ["ABC_1", "ABC_2", "ABC_3", "ABC_4","ABC_5","ABC_6"],
    "strem_nm": ["STREM_ABC"] * 6,
    "prir": [1, 2, 2, 3, 3, 4]
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

START_STREM = SparkKubernetesOperator(
    task_id='START_STREM',
    application_file="start_strem.yaml",
    do_xcom_push=True,
    api_group="sparkoperator.hpe.com",
    enable_impersonation_from_ldap_user=True,
    params={'STREM_NM': 'STREM_ABC'},
    dag=dag,
)

START_STREM_MONITOR = SparkKubernetesSensor(
    task_id='START_STREM_MONITOR',
    application_name="{{ ti.xcom_pull(task_ids='START_STREM')['metadata']['name'] }}",
    dag=dag,
    api_group="sparkoperator.hpe.com",
    attach_log=True,
    do_xcom_push=True,
)

# Dictionary to hold references to the tasks
tasks = {}

# List to hold task groups
task_groups = []

# Iterate over the DataFrame rows
for index, row in df.iterrows():
    task_id = f"{row['prcs_nm']}"
    
    # Create SparkKubernetesOperator for each row
    task = SparkKubernetesOperator(
        task_id=task_id,
        application_file="test_cntl.yaml",
        do_xcom_push=True,
        params={"PRCS_NM": row['prcs_nm']},
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

    # Append the task and its monitor task to the appropriate group based on priority
    if not task_groups or task_groups[-1][0]['prir'] != row['prir']:
        task_groups.append([])
    task_groups[-1].append({'task': task, 'monitor_task': monitor_task, 'prir': row['prir']})

# Set up dependencies between task groups
for i in range(len(task_groups) - 1):
    group = task_groups[i]
    next_group = task_groups[i + 1]
    for task_info in group:
        task_info['task'] >> task_info['monitor_task']
        for next_task_info in next_group:
            task_info['monitor_task'] >> next_task_info['task']

# Set up the initial dependency
for task_info in task_groups[0]:
    task_info['task'] >> task_info['monitor_task']

# Set up the final dependency for the last monitor task
for task_info in task_groups[-1]:
    if task_info['prir'] == df['prir'].max():
        task_info['task'] >> task_info['monitor_task']
        
# Print the tasks for verification
print(tasks)