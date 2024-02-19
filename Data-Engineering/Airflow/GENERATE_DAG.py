from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from datetime import datetime

# Define your DataFrame or data
data = [
    {"prcs_nm": "ABC_1", "strem_nm": "STREM_ABC", "prir": 1},
    {"prcs_nm": "ABC_2", "strem_nm": "STREM_ABC", "prir": 2},
    {"prcs_nm": "ABC_3", "strem_nm": "STREM_ABC", "prir": 2}
]

def generate_dag(dag_id, schedule_interval):
    # Define your default arguments for the DAG
    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2024, 2, 19),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1
    }

    # Instantiate your DAG
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,  # You can set the schedule interval as needed
    )

    # Create dictionary to store tasks
    tasks = {}

    # Loop through the data and create tasks dynamically
    for row in data:
        prcs_nm = row['prcs_nm']
        prir = row['prir']

        # Define the SparkKubernetesOperator task
        tasks[prcs_nm] = SparkKubernetesOperator(
            task_id=f'task_{prcs_nm}',
            application_file="start_strem.yaml",
            do_xcom_push=True,
            params={"PRCS_NM": prcs_nm},
            dag=dag,
            api_group="sparkoperator.hpe.com",
            enable_impersonation_from_ldap_user=True
        )

        # Define the SparkKubernetesSensor task
        tasks[f'{prcs_nm}_monitor'] = SparkKubernetesSensor(
            task_id=f'task_{prcs_nm}_monitor',
            application_name="{{ task_instance.xcom_pull(task_ids='" + f'task_{prcs_nm}' + "')['metadata']['name'] }}",
            dag=dag,
            api_group="sparkoperator.hpe.com",
            attach_log=True
        )

        # Set task dependencies based on priority
        if prir > 1:
            tasks[f'task_{prcs_nm}'] >> tasks[f'task_{prcs_nm}_monitor']
        else:
            tasks['task_ABC_1'] >> tasks['task_ABC_1_monitor'] >> tasks[f'task_{prcs_nm}'] >> tasks[f'task_{prcs_nm}_monitor']

    return dag

# Generate the DAG
dag_id = 'dynamic_spark_dag'
schedule_interval = None  # You can set the schedule interval as needed
generated_dag = generate_dag(dag_id, schedule_interval)
