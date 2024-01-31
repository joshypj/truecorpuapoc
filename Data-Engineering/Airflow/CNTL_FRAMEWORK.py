# Import required modules
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Param
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from pyspark.sql import SparkSession
from py4j.java_gateway import java_import
java_import(spark._sc._jvm, "org.apache.spark.sql.api.python.*")
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType,DateType
from pyspark.sql.functions import to_date,col,when  



# Define the configuration
configuration = {"conf": {"spark.driver.cores": 2
          , "spark.executor.cores": 2
          , "spark.driver.memory": "1000M"
          , "spark.executor.memory": "1000M"
          , "spark.ssl.enabled": False
          , "spark.kubernetes.driver.volumes.persistentVolumeClaim.pv1.options.claimName": "kubeflow-shared-pvc"
          , "spark.kubernetes.driver.volumes.persistentVolumeClaim.pv1.mount.path": "/mounts/shared-volume/shared"
          , "spark.kubernetes.driver.volumes.persistentVolumeClaim.pv1.mount.readOnly": False
          , "spark.kubernetes.executor.volumes.persistentVolumeClaim.pv1.options.claimName": "kubeflow-shared-pvc"
          , "spark.kubernetes.executor.volumes.persistentVolumeClaim.pv1.mount.path": "/mounts/shared-volume/shared"
          , "spark.kubernetes.executor.volumes.persistentVolumeClaim.pv1.mount.readOnly": False
          , "spark.kubernetes.driver.volumes.persistentVolumeClaim.upv1.options.claimName": "ezadmin-spark-pvc"
          , "spark.kubernetes.driver.volumes.persistentVolumeClaim.upv1.mount.path": "/mounts/shared-volume/user"
          , "spark.kubernetes.driver.volumes.persistentVolumeClaim.upv1.mount.readOnly": False
          , "spark.kubernetes.executor.volumes.persistentVolumeClaim.upv1.options.claimName": "ezadmin-spark-pvc"
          , "spark.kubernetes.executor.volumes.persistentVolumeClaim.upv1.mount.path": "/mounts/shared-volume/user"
          , "spark.kubernetes.executor.volumes.persistentVolumeClaim.upv1.mount.readOnly": False
          , "spark.dynamicAllocation.enabled": True, "spark.dynamicAllocation.shuffleTracking.enabled": True
          , "spark.dynamicAllocation.minExecutors": 1
          , "spark.dynamicAllocation.maxExecutors": 4
          , "spark.kubernetes.container.image": "gcr.io/mapr-252711/spark-3.4.0:v3.4.0"
          , "spark.kubernetes.container.image.pullPolicy": "Always"
          , "spark.kubernetes.driverEnv.PRESTO_ACCESS_TOKEN": "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJwNTdWNHFEVldrTlp6Zkw4Z0h1STNmRHdtTFQ0VjlMN05wWDFMWkdOWjg4In0.eyJleHAiOjE3MDY4NDYzNzksImlhdCI6MTcwNjI0MTU3OSwiYXV0aF90aW1lIjoxNzA2MjQxNTc5LCJqdGkiOiIzNmIwNzg3ZS1hNWJjLTQ5MTEtYWVlYy1hMDVlMGVmNjlkOTgiLCJpc3MiOiJodHRwczovL2tleWNsb2FrLnRydWVwb2MuZXphcGFjLmNvbS9yZWFsbXMvVUEiLCJhdWQiOiJ1YSIsInN1YiI6ImQxYTliNTNjLWEzNGItNDUzNy05YTMzLWNjZjBjZDFjY2VjMSIsInR5cCI6IklEIiwiYXpwIjoidWEiLCJub25jZSI6IlNfZFpnWnMzdU9fQmZINklxNWxuYTA0YllrT0xSSHlYZVZSNWNHd3VmRmMiLCJzZXNzaW9uX3N0YXRlIjoiMjBmNjAyZGUtNjQzNC00ZjYzLTkzYWYtODI4MWVhOTZmNDNlIiwiYXRfaGFzaCI6IjQ3Z19iNU1ab0xQWTdSOUNpZFk0ckEiLCJhY3IiOiIxIiwic2lkIjoiMjBmNjAyZGUtNjQzNC00ZjYzLTkzYWYtODI4MWVhOTZmNDNlIiwidWlkIjoiNjAwMCIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiZ2lkIjoiNTAwNSIsIm5hbWUiOiJlemFkbWluIHRlc3QiLCJncm91cHMiOlsidWEtZW5hYmxlZCIsIm9mZmxpbmVfYWNjZXNzIiwiYWRtaW4iLCJ1bWFfYXV0aG9yaXphdGlvbiIsImRlZmF1bHQtcm9sZXMtdWEiXSwicHJlZmVycmVkX3VzZXJuYW1lIjoiZXphZG1pbiIsImdpdmVuX25hbWUiOiJlemFkbWluIiwicG9zaXhfdXNlcm5hbWUiOiJlemFkbWluIiwiZmFtaWx5X25hbWUiOiJ0ZXN0IiwiZW1haWwiOiJlemFkbWluQGV6YXBhYy5jb20ifQ.QWO5Lfyj0vMJEi21_69mv0PgVk7ZTqAsrGEk-7zyqC8SA-IOrR5AuIJ9ESzL-O9rgaPyp11wW6TW5HEBrAotDDQrgBGP3HFPY9TKpCOlXkKzq9zretDLvR_Ya_qKGph16MMXV8uDUShEqi8x1KdHfyleEZpqd1Y98c6ckTQgAt_H1lrcGVEADT6Mme_WgYLdHxK-nVWHwlzIeDszXTfta_Wap2SgrDZIQpLlxWozqZyABZo-DVtqp3z_s6sBYWQHmRwedH6GBMxr9EUA5w6Mmu0t1s54_YX24RFy34B4JnYDSkCZKO8blJeXIQEJqjwyvl4UT7n0Zp5wDevxYPqVOg"
          , "spark.executorEnv.PRESTO_ACCESS_TOKEN": "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJwNTdWNHFEVldrTlp6Zkw4Z0h1STNmRHdtTFQ0VjlMN05wWDFMWkdOWjg4In0.eyJleHAiOjE3MDY4NDYzNzksImlhdCI6MTcwNjI0MTU3OSwiYXV0aF90aW1lIjoxNzA2MjQxNTc5LCJqdGkiOiIzNmIwNzg3ZS1hNWJjLTQ5MTEtYWVlYy1hMDVlMGVmNjlkOTgiLCJpc3MiOiJodHRwczovL2tleWNsb2FrLnRydWVwb2MuZXphcGFjLmNvbS9yZWFsbXMvVUEiLCJhdWQiOiJ1YSIsInN1YiI6ImQxYTliNTNjLWEzNGItNDUzNy05YTMzLWNjZjBjZDFjY2VjMSIsInR5cCI6IklEIiwiYXpwIjoidWEiLCJub25jZSI6IlNfZFpnWnMzdU9fQmZINklxNWxuYTA0YllrT0xSSHlYZVZSNWNHd3VmRmMiLCJzZXNzaW9uX3N0YXRlIjoiMjBmNjAyZGUtNjQzNC00ZjYzLTkzYWYtODI4MWVhOTZmNDNlIiwiYXRfaGFzaCI6IjQ3Z19iNU1ab0xQWTdSOUNpZFk0ckEiLCJhY3IiOiIxIiwic2lkIjoiMjBmNjAyZGUtNjQzNC00ZjYzLTkzYWYtODI4MWVhOTZmNDNlIiwidWlkIjoiNjAwMCIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiZ2lkIjoiNTAwNSIsIm5hbWUiOiJlemFkbWluIHRlc3QiLCJncm91cHMiOlsidWEtZW5hYmxlZCIsIm9mZmxpbmVfYWNjZXNzIiwiYWRtaW4iLCJ1bWFfYXV0aG9yaXphdGlvbiIsImRlZmF1bHQtcm9sZXMtdWEiXSwicHJlZmVycmVkX3VzZXJuYW1lIjoiZXphZG1pbiIsImdpdmVuX25hbWUiOiJlemFkbWluIiwicG9zaXhfdXNlcm5hbWUiOiJlemFkbWluIiwiZmFtaWx5X25hbWUiOiJ0ZXN0IiwiZW1haWwiOiJlemFkbWluQGV6YXBhYy5jb20ifQ.QWO5Lfyj0vMJEi21_69mv0PgVk7ZTqAsrGEk-7zyqC8SA-IOrR5AuIJ9ESzL-O9rgaPyp11wW6TW5HEBrAotDDQrgBGP3HFPY9TKpCOlXkKzq9zretDLvR_Ya_qKGph16MMXV8uDUShEqi8x1KdHfyleEZpqd1Y98c6ckTQgAt_H1lrcGVEADT6Mme_WgYLdHxK-nVWHwlzIeDszXTfta_Wap2SgrDZIQpLlxWozqZyABZo-DVtqp3z_s6sBYWQHmRwedH6GBMxr9EUA5w6Mmu0t1s54_YX24RFy34B4JnYDSkCZKO8blJeXIQEJqjwyvl4UT7n0Zp5wDevxYPqVOg"
          , "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
          , "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
          , "spark.executor.extraJavaOptions": "-Dcom.amazonaws.sdk.disableCertChecking=true"
          , "spark.driver.extraJavaOptions": "-Dcom.amazonaws.sdk.disableCertChecking=true"
          , "spark.hadoop.fs.s3a.path.style.access": True
          , "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
          , "spark.hadoop.fs.s3a.access.key": "PO93KAAP2YTG4RHMG499FENAFWYUH9Q91XOJYQ6UNY4R0XUVY35A53L47"
          , "spark.hadoop.fs.s3a.secret.key": "UV0H7MFJI96MCFY03IMD9X6ZT8H43B3GDS8SXGA4Z7FH0QT2DSZV3ZCKTHS93Z"
          ,"spark.hadoop.fs.s3a.endpoint": "https://54.251.141.200:9000"
         }}

def execute_spark_task(**kwargs):
    # Extract configuration from kwargs
    strem_nm = kwargs['dag_run'].conf.get('STREM_NM')
    
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("ETL") \
        .config(conf=configuration['conf']) \
        .getOrCreate()
    
    strem_df = spark.read.format("jdbc").\
      option("driver", "com.facebook.presto.jdbc.PrestoDriver").\
      option("url", "jdbc:presto://ezpresto.truepoc.ezapac.com:443").\
      option("SSL", "true").\
      option("IgnoreSSLChecks", "true").\
      option("query", f"SELECT * FROM DEV_DELTA.DEV_TESTING.CNTL_CFG_STREM WHERE STREM_NM = '{strem_nm}' AND ACT_F = '1'").\
      load()
    
    act_strem = strem_df.collect()[0].strem_nm
    data_dt_strem = strem_df.collect()[0].data_dt
    formatted_date = data_dt_strem.strftime("%Y-%m-%d")
    frq = strem_df.collect()[0].frq

    prcs_df = spark.read.format("jdbc").\
        option("driver", "com.facebook.presto.jdbc.PrestoDriver").\
        option("url", "jdbc:presto://ezpresto.truepoc.ezapac.com:443").\
        option("SSL", "true").\
        option("IgnoreSSLChecks", "true").\
        option("query", f"SELECT * FROM DEV_DELTA.DEV_TESTING.CNTL_CFG_PRCS WHERE STREM_NM = '{act_strem}' AND ACT_F = '1' ORDER BY CAST(PRIR AS INT)").\
        load()

    fi_df = spark.read.format("jdbc").\
                        option("driver", "com.facebook.presto.jdbc.PrestoDriver").\
                        option("url", "jdbc:presto://ezpresto.truepoc.ezapac.com:443").\
                        option("SSL", "true").\
                        option("IgnoreSSLChecks", "true").\
                        option("query", f"SELECT * FROM DEV_DELTA.DEV_TESTING.CNTL_CFG_FI WHERE ACT_F = '1'").\
                        load()

    column = ['fi_id', 'fi_nm', 'dlm_strng', 'sht_nm', 'source_path', 'dest_path', 'schm_fi']
    fi_df = fi_df.select(column)

    df = prcs_df.join(fi_df, prcs_df["fi_id"] == fi_df["fi_id"],"left_outer")

    cntl_prcs_log_schema = StructType([
        StructField("PRCS_NM", StringType(), True),
        StructField("LD_ID", StringType(), True),
        StructField("STREM_ID", StringType(), True),
        StructField("DATA_DT", StringType(), True),
        StructField("STRT_DTTM", StringType(), True),
        StructField("END_DTTM", StringType(), True),
        StructField("ST", StringType(), True),
        StructField("RMRK", StringType(), True),
        StructField("UPDT_DTTM", StringType(), True),
        StructField("UPDT_BY", StringType(), True)
    ])
    delta_path_cntl_prcs_log = 's3a://truepoc-bkt-raw/dev_testing/cntl_prcs_log'

    for prcs in df.collect():
        max_ld_id = spark.read.format("jdbc").\
        option("driver", "com.facebook.presto.jdbc.PrestoDriver").\
        option("url", "jdbc:presto://ezpresto.truepoc.ezapac.com:443").\
        option("SSL", "true").\
        option("IgnoreSSLChecks", "true").\
        option("query", f"select  MAX(LD_ID) AS LD_ID  from dev_delta.dev_testing.cntl_prcs_log  where prcs_nm = '{prcs.prcs_nm}' group by PRCS_NM").\
        load()
        
        if len(max_ld_id.collect()) == 0:
            max_ld_id = 1
            print(max_ld_id)
        else:
            max_ld_id = int(max_ld_id.collect()[0].STREM_ID) + 1
            print(max_ld_id)
            
        params = prcs.parm
        prcs_type = prcs.prcs_typ
            
        # script_insert = f"insert into dev_delta.dev_testing.cntl_prcs_log VALUES ('{prcs.prcs_nm}','{max_ld_id}','{max_strem_id}','{formatted_date}','{start_notebook}','','SUCCESS','','','NOTEBOOK' )"
        # insert_sql_to_spark(script_insert,delta_path_cntl_prcs_log,cntl_prcs_log_schema,'PRCS_LOG')

    print(params,prcs_type)

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'CNTL_FRAMEWORK',
    default_args=default_args,
    description='Running Stream',
    schedule_interval=None,
    tags=['e2e example', 'ETL', 'spark'],
    params={
        'STREM_NM': Param("X3_TEST_99_D", type="string"),
    },
    access_control={
        'All': {
            'can_read',
            'can_edit',
            'can_delete'
        }
    }
)

# Define Python functions
def start_job():
    print("Start ETL by Query...")

def end_job():
    print("Data insert to Table Done...")

# Define the tasks
task1 = PythonOperator(
    task_id='Start_Data_Reading',
    python_callable=execute_spark_task,
    dag=dag,
)

def _set_arguments(**kwargs):
    arguments_to_pass = {
        'strem_nm': kwargs['dag_run'].conf.get('STREM_NM')
    }
    return arguments_to_pass

set_arguments = PythonOperator(
    task_id='set_arguments',
    python_callable=_set_arguments,
    provide_context=True,
    dag=dag,
)

def _get_arguments(**kwargs):
    ti = kwargs['ti']
    arguments_to_pass = ti.xcom_pull(task_ids='set_arguments')
    print(arguments_to_pass)
    return arguments_to_pass['strem_nm']

# argument_to_pass = {'strem_nm' : "TEST"}

task2 = SparkKubernetesOperator(
    task_id='Spark_etl_submit',
    application_file="CNTL_FRAMEWORK.yaml",
    do_xcom_push=True,
    api_group="sparkoperator.hpe.com",
    enable_impersonation_from_ldap_user=True,
    dag = dag,
)

task3 = SparkKubernetesSensor(
    task_id='Spark_etl_monitor',
    application_name="{{ ti.xcom_pull(task_ids='Spark_etl_submit')['metadata']['name'] }}",
    dag=dag,
    api_group="sparkoperator.hpe.com",
    attach_log=True,
    do_xcom_push=True,
)

task4 = PythonOperator(
    task_id='Data_Loading_Done',
    python_callable=end_job,
    dag=dag,
)

# Define task dependencies
task1 >>set_arguments>> task2 >> task3 >> task4
