from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
import pandas as pd
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
import json


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'STREM_ABC',
    default_args=default_args,
    description='Create dynamic DAGs',
    schedule_interval='0 6 * * *',  # Schedule to run daily at 6:00 AM
    tags=['e2e example','ETL', 'spark'],
    params={"STREM_NM": 'STREM_ABC',"RUN_MODE" : 'F'},
    access_control={
        'All': {
            'can_read',
            'can_edit',
            'can_delete'
        }
    }
)
strem_nm = dag.params.get("STREM_NM", None)
run_mode = dag.params.get("RUN_MODE", None)

def check_dpnd(dpnd_prcs_nm, log_df):
    if dpnd_prcs_nm in log_df['prcs_nm'].unique().tolist():
        # Filter rows where prcs_nm is dpnd_prcs_nm
        dpnd_prcs_nm_df = log_df[log_df['prcs_nm'] == dpnd_prcs_nm]

        # Check if the filtered DataFrame is empty
        if not dpnd_prcs_nm_df.empty:
            # Find the row with the maximum ld_id
            max_ld_id_row = dpnd_prcs_nm_df[dpnd_prcs_nm_df['ld_id'] == dpnd_prcs_nm_df['ld_id'].max()]

            # Check if the filtered DataFrame contains rows
            if not max_ld_id_row.empty:
                # Get the status of the prcs_nm = dpnd_prcs_nm with the maximum ld_id
                status = max_ld_id_row['st'].values[0]
                if status == 'SUCCESS':
                    return True
                else:
                    raise Exception("Status is not SUCCESS")
            else:
                raise Exception("No rows found with the maximum ld_id")
        else:
            raise Exception(f"No rows found with prcs_nm = {dpnd_prcs_nm}")
    else:
        raise Exception(f"{dpnd_prcs_nm} not found in prcs_nm column")


# Create or read your DataFrame
variable_data  = Variable.get("dpnd_df")
data = json.loads(variable_data)
df = pd.DataFrame(data)

variable_data = Variable.get("prcs_log_df")
data = json.loads(variable_data)
log_df  =pd.DataFrame(data)

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
        trigger_rule = 'none_failed',
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
        trigger_rule = 'none_failed',
        dag=dag,
        api_group="sparkoperator.hpe.com",
        attach_log=True
    )
    dpnd_prcs_nm_l = []
    for dpnd_prcs_nm in df.loc[df['prcs_nm']==prcs_nm]['dpnd_prcs_nm'].unique().tolist() :
        if dpnd_prcs_nm != None and dpnd_prcs_nm not in dpnd_prcs_nm_s and dpnd_prcs_nm not in df['prcs_nm'].unique().tolist() :
            wait_task = PythonOperator(
                task_id = f"wait_{dpnd_prcs_nm}",
                python_callable=check_dpnd,  # Pass the reference without calling the function
                trigger_rule = 'none_failed',
                op_args=[dpnd_prcs_nm, log_df],  # Pass arguments if needed
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