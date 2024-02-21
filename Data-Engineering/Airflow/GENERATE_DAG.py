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
    "prcs_nm": ["ABC_1", "ABC_2", "ABC_3", "ABC_3", "ABC_4", "ABC_4"],
    "dpnd_prcs_nm": [None, "ABC_1", "ABC_1", "XY_1", "ABC_1", "ABC_2"]
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

# List to hold task groups
task_groups = []
dpnd_prcs_nm_s = {}
# Iterate over the DataFrame rows
for prcs_nm in df['prcs_nm'].unique().tolist():
    task_id = prcs_nm
    
    # Create SparkKubernetesOperator for each row
    task = SparkKubernetesOperator(
        task_id=task_id,
        application_file="test_cntl.yaml",
        do_xcom_push=True,
        params={"PRCS_NM": prcs_nm},
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
    dpnd_prcs_nm_l = []
    for dpnd_prcs_nm in df.loc[df['prcs_nm']==prcs_nm]['dpnd_prcs_nm'].unique().tolist() :
        if dpnd_prcs_nm != None and dpnd_prcs_nm not in dpnd_prcs_nm_s and dpnd_prcs_nm not in df['prcs_nm'].unique().tolist() :
            wait_task = DummyOperator(
                task_id = f"wait_{dpnd_prcs_nm}",
                dag = dag
            )
            dpnd_prcs_nm_l.append(wait_task)
            dpnd_prcs_nm_s[dpnd_prcs_nm] = wait_task
            
        elif dpnd_prcs_nm != None and dpnd_prcs_nm in dpnd_prcs_nm_s and dpnd_prcs_nm not in df['prcs_nm'].unique().tolist() : 
             dpnd_prcs_nm_l.append(dpnd_prcs_nm_s[dpnd_prcs_nm])
         
        elif dpnd_prcs_nm != None and dpnd_prcs_nm  in df['prcs_nm'].unique().tolist() :
            for i in task_groups :
                if i['prcs_nm'] == dpnd_prcs_nm :
                    dpnd_prcs_nm_l.append(i['monitor_task'])
    
    task_groups.append({'prcs_nm' : prcs_nm,'task': task, 'monitor_task': monitor_task, 'dpnd': dpnd_prcs_nm_l})


# Set up dependencies between task groups
for i in task_groups:
    for j in i['dpnd'] :
        j >> i['task']
    i['task'] >> i['monitor_task']
        
# Print the tasks for verification
print(tasks)