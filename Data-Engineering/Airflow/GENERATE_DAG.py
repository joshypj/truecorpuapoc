from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
import pandas as pd

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

# Create a dummy task to check dependency status
check_dependency_status = PythonOperator(
    task_id='check_dependency_status',
    python_callable=lambda: None,
    dag=dag,
)

# Iterate over the DataFrame rows
for index, row in df.iterrows():
    prcs_nm = row['prcs_nm']
    dpnd_prcs_nm = row['dpnd_prcs_nm']

    # Create SparkKubernetesOperator for prcs_nm
    task_id = f"{prcs_nm}"
    task = SparkKubernetesOperator(
        task_id=task_id,
        application_file="test_cntl.yaml",
        do_xcom_push=True,
        params={"PRCS_NM": prcs_nm},
        dag=dag,
        api_group="sparkoperator.hpe.com",
        enable_impersonation_from_ldap_user=True
    )
    tasks[task_id] = task

    # Create SparkKubernetesSensor for prcs_nm
    monitor_task = SparkKubernetesSensor(
        task_id=f"{task_id}_monitor",
        application_name=f"{{{{ task_instance.xcom_pull(task_ids='{task_id}')['metadata']['name'] }}}}",
        dag=dag,
        api_group="sparkoperator.hpe.com",
        attach_log=True
    )

    # Set dependency for prcs_nm >> prcs_nm_monitor
    task >> monitor_task

    # Check if dpnd_prcs_nm is not None and it exists in the tasks
    if dpnd_prcs_nm is not None and dpnd_prcs_nm in tasks:
        # Set the dependency if dpnd_prcs_nm is in the tasks
        tasks[dpnd_prcs_nm] >> task

    else:
        # Set the dependency on the dummy check_dependency_status task
        check_dependency_status >> task

# Print the tasks for verification
print(tasks)
