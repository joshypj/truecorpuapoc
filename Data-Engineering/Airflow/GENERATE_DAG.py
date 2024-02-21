from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
import pandas as pd
from airflow.hooks.presto_hook import PrestoHook

# Create or read your DataFrame
data = {
    "prcs_nm": ["ABC_1", "ABC_2", "ABC_3", "ABC_4","ABC_5","ABC_6"],
    "strem_nm": ["STREM_ABC"] * 6,
    "prir": [1, 2, 2, 3, 3, 4]
}
df = pd.DataFrame(data)

presto_hook = PrestoHook(presto_conn_id='eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJwNTdWNHFEVldrTlp6Zkw4Z0h1STNmRHdtTFQ0VjlMN05wWDFMWkdOWjg4In0.eyJleHAiOjE3MDg1NjcwMjMsImlhdCI6MTcwNzk2MjIyMywiYXV0aF90aW1lIjoxNzA3OTYyMjIzLCJqdGkiOiJhNGIzOGU2ZS1jYmVjLTQxMWYtYjVlNy0yZjFkNTczMjQzZjgiLCJpc3MiOiJodHRwczovL2tleWNsb2FrLnRydWVwb2MuZXphcGFjLmNvbS9yZWFsbXMvVUEiLCJhdWQiOiJ1YSIsInN1YiI6ImQxYTliNTNjLWEzNGItNDUzNy05YTMzLWNjZjBjZDFjY2VjMSIsInR5cCI6IklEIiwiYXpwIjoidWEiLCJub25jZSI6IldVSVdsWmZvVFliOGRuZEVmUkpodWF1MHMwVWliVENWeFRWVE1UVTdzMjQiLCJzZXNzaW9uX3N0YXRlIjoiNzdlZjIzN2EtMWJlOS00NTZkLWFhOTQtMmE4ZGZhNTI5M2JiIiwiYXRfaGFzaCI6IjlVemViX2I4ZTY5cUtYRkN5UjFLSmciLCJhY3IiOiIxIiwic2lkIjoiNzdlZjIzN2EtMWJlOS00NTZkLWFhOTQtMmE4ZGZhNTI5M2JiIiwidWlkIjoiNjAwMCIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiZ2lkIjoiNTAwNSIsIm5hbWUiOiJlemFkbWluIHRlc3QiLCJncm91cHMiOlsidWEtZW5hYmxlZCIsIm9mZmxpbmVfYWNjZXNzIiwiYWRtaW4iLCJ1bWFfYXV0aG9yaXphdGlvbiIsImRlZmF1bHQtcm9sZXMtdWEiXSwicHJlZmVycmVkX3VzZXJuYW1lIjoiZXphZG1pbiIsImdpdmVuX25hbWUiOiJlemFkbWluIiwicG9zaXhfdXNlcm5hbWUiOiJlemFkbWluIiwiZmFtaWx5X25hbWUiOiJ0ZXN0IiwiZW1haWwiOiJlemFkbWluQGV6YXBhYy5jb20ifQ.FPlrCasM3CFq1ip10ZIkX_fw0-2cg7uARZhciW9gAxjhp4uoU6lkCJEJiyRkkpqyVDPPLHAjU1Tgv70rlFsR-dLjwWeyHfHR04pGoG2PmsolovWYTxFhILQtoT9rrFfY4tmOXdoxkJYrO1SNn5hdq5BDkvCiodl0FukojbiS4n_f2zgfYTeB5dEAIwcH04fEuG6X5JATe9wdz7xQNbbSV2brNQw1IartBm9HsnRza7RaJHe9bd-3q4J5Xp25ddS0IYPow6fi6DeMwCIDu0G9j7FnsjxXEeee86gGjCZwdwGMd0YM6XnAfs-uFsTZMweEuPynBgnvUBKsRqEwQjGQ-g')
sql_query = "SELECT * FROM your_table;"
records = presto_hook.get_records(sql_query)



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

df = pd.read_csv(filepath)


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