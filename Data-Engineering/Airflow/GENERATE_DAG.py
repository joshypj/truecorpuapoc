from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
import pandas as pd
from airflow.operators.dummy_operator import DummyOperator

# Create or read your DataFrame
data = {
    "prcs_nm": ["ABC_1", "ABC_2", "ABC_3","ABC_3", "ABC_4","ABC_4"],
    "dpnd_prcs_nm": [None,"ABC_1","ABC_1","XY_1","ABC_2","ABC_3"]
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


# Dictionary to hold references to the tasks
tasks = {}

# Dummy operator for checking dependency process status
check_dependency_status = DummyOperator(
    task_id='CHECK_DEPENDENCY_STATUS',
    dag=dag
)

# Iterate over the DataFrame rows
for index, row in df.iterrows():
    task_id = row['prcs_nm']

    # Create SparkKubernetesOperator for each row
    task = SparkKubernetesOperator(
        task_id=task_id,
        application_file="test_cntl.yaml",
        do_xcom_push=True,
        params={"PRCS_NM": task_id},
        dag=dag,
        api_group="sparkoperator.hpe.com",
        enable_impersonation_from_ldap_user=True
    )

    # Add the task to the tasks dictionary
    tasks[task_id] = task

    # Create SparkKubernetesSensor for each row
    monitor_task = SparkKubernetesSensor(
        task_id=f"{task_id}_monitor",
        application_name="{{ task_instance.xcom_pull(task_ids='" + task_id + "') }}",
        dag=dag,
        api_group="sparkoperator.hpe.com",
        attach_log=True
    )

    # Set upstream dependencies
    if row['dpnd_prcs_nm']:
        if row['dpnd_prcs_nm'] in tasks:
            tasks[row['dpnd_prcs_nm']] >> monitor_task
        else:
            tasks[row['dpnd_prcs_nm']] = check_dependency_status
            tasks[row['dpnd_prcs_nm']] >> monitor_task
    else:
        check_dependency_status >> monitor_task

# Print the tasks for verification
print(tasks)