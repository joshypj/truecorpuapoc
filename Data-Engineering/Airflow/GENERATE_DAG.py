from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
import pandas as pd
import boto3
import botocore
import os
import time
# Create or read your DataFrame
data = {
    "prcs_nm": ["ABC_1", "ABC_2", "ABC_3", "ABC_4","ABC_5","ABC_6"],
    "strem_nm": ["STREM_ABC"] * 6,
    "prir": [1, 2, 2, 3, 3, 4]
}
df = pd.DataFrame(data)

aws_access_key_id = 'eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJwNTdWNHFEVldrTlp6Zkw4Z0h1STNmRHdtTFQ0VjlMN05wWDFMWkdOWjg4In0.eyJleHAiOjE3MDkwMTQxMjcsImlhdCI6MTcwODQwOTMyOCwiYXV0aF90aW1lIjoxNzA4NDA5MzI3LCJqdGkiOiIyNjYzMjgyNS1mNjdjLTQ5ZjQtOTQxNS04ZjJlZTZmMWRlMmIiLCJpc3MiOiJodHRwczovL2tleWNsb2FrLnRydWVwb2MuZXphcGFjLmNvbS9yZWFsbXMvVUEiLCJhdWQiOiJ1YSIsInN1YiI6ImQxYTliNTNjLWEzNGItNDUzNy05YTMzLWNjZjBjZDFjY2VjMSIsInR5cCI6IklEIiwiYXpwIjoidWEiLCJub25jZSI6IjZKVi1NN2VXYi1aeF83UFJOcUJCNW1zLUNFM0pmWHBnM3BkNXRkVmR5YkEiLCJzZXNzaW9uX3N0YXRlIjoiY2M5ZDUzNTMtMWQxMC00NTc0LThmNDItM2YyOTlmNzNlMTExIiwiYXRfaGFzaCI6IjZHS19RZDVnZmViTjZhVVE5Mkxka0EiLCJhY3IiOiIxIiwic2lkIjoiY2M5ZDUzNTMtMWQxMC00NTc0LThmNDItM2YyOTlmNzNlMTExIiwidWlkIjoiNjAwMCIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiZ2lkIjoiNTAwNSIsIm5hbWUiOiJlemFkbWluIHRlc3QiLCJncm91cHMiOlsidWEtZW5hYmxlZCIsIm9mZmxpbmVfYWNjZXNzIiwiYWRtaW4iLCJ1bWFfYXV0aG9yaXphdGlvbiIsImRlZmF1bHQtcm9sZXMtdWEiXSwicHJlZmVycmVkX3VzZXJuYW1lIjoiZXphZG1pbiIsImdpdmVuX25hbWUiOiJlemFkbWluIiwicG9zaXhfdXNlcm5hbWUiOiJlemFkbWluIiwiZmFtaWx5X25hbWUiOiJ0ZXN0IiwiZW1haWwiOiJlemFkbWluQGV6YXBhYy5jb20ifQ.SyFgstM42nkhAmoez-UrPxBN8cRgiOza7Yn_vLKsre62Gin5IscAQp2kcFJGFDuZchzzTr7t8n1CnuAh6gKA1etpmkLAoYijtuyB9s4UiDcgKMPZbWdnis74jtU-EslwCy223TjYUytnrZwgqSTn3N1d1gzVMesp26Kcsm6WOQXyNpTOpAlDrOzTfF8lApSAkR6fQ-AEkC78wxhVRj_VgGBLbENv8yy7tVULX3xpbUmo5vYXUvjVFT9sK2NTGexqco9jlPa5_oFtb47T8iibGCVkaoSdkVqrwS1W7LmxPEDFi6DCc20iyd5AyRGqPHbPNthP63JfpP_un-5_8z3qcg'
aws_secret_access_key = 's3abc'
aws_region = 'us-east-2'

print("Value of aws_access_key_id is: " + aws_access_key_id + "\n")


S3_PROXY_SERVICE_SUFFIX_URL = '-service.ezdata-system.svc.cluster.local:30000'
ON_PREM_S3_SERVICE_NAME = 'ezmerals3'
on_prem_bucket_name = 'my-bucket'

AWS_S3_SERVICE_NAME = 'awss3'
aws_bucket_name = 'ezaf-prakash-test-01'


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