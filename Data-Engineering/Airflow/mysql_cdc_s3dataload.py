print('""""""""""""""""""""""""""""""""""""""""""""""""""""""""""')
print('""""""""""""""""Executing the python kafka topic read script"""""""""""""""')
# Import necessary libraries
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local")\
            .appName("ETL").getOrCreate()
spark.sparkContext.setLogLevel("ALL")
from py4j.java_gateway import java_import
java_import(spark._sc._jvm, "org.apache.spark.sql.api.python.*")
from pyspark.sql.functions import from_json
from delta.tables import DeltaTable

from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.types import *



# Define the folder path
delta_path = "s3a://truecorpcdc/mysql_orders4/"
# Connects to EzPresto over SSL, executes a SQL query, and loads the result into a Spark DataFrame
bootstrap_servers = "10.103.92.127:9092"
topic = 'mysql-tpcc-.tpcc.orders'


df = (
    spark
    .readStream
    .format("kafka")
    .option("maxFilesPerTrigger", 1000)
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("subscribe", topic)
    .option("kafka.security.protocol", "PLAINTEXT")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.username", 'user1')
    .option("kafka.sasl.password", 'user1') 
    .option("includeHeaders", "true") 
    .option("startingOffsets", "earliest")
    .load()
)



jsonSchema = StructType([StructField('payload',StructType([StructField('before',StringType(),True),  
StructField('after',StructType([StructField('o_id',StringType(),True),  
StructField('o_w_id',StringType(),True),  
StructField('o_d_id',StringType(),True),  
StructField('o_c_id',StringType(),True),  
StructField('o_carrier_id',StringType(),True),  
StructField('o_ol_cnt',StringType(),True),  
StructField('o_all_local',StringType(),True),  
StructField('o_entry_d',StringType(),True)]),True)]),True)])


extractedDF = df \
    .selectExpr("CAST(value AS STRING)","timestamp") \
    .select(from_json("value", jsonSchema).alias("data"),"timestamp") \
    .select("data.payload.after.*", "timestamp") 

extractedDF_after = extractedDF \
    .selectExpr("CAST(o_id AS INTEGER)","CAST(o_w_id AS INTEGER)","CAST(o_d_id AS INTEGER)","CAST(o_c_id AS INTEGER)","CAST(o_carrier_id AS INTEGER)","CAST(o_ol_cnt AS INTEGER)","CAST(o_all_local AS INTEGER)","CAST(o_entry_d AS INTEGER)","timestamp")

query = (
    extractedDF_after
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("maxFilesPerTrigger", 1000)
    .option("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .option("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .option("spark.executor.extraJavaOptions", "-Dcom.amazonaws.sdk.disableCertChecking=true")
    .option("spark.driver.extraJavaOptions", "-Dcom.amazonaws.sdk.disableCertChecking=true")
    .option("spark.hadoop.fs.s3a.path.style.access", "false")
    .option("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    .option("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .option("spark.hadoop.fs.s3a.access.key", "PO93KAAP2YTG4RHMG499FENAFWYUH9Q91XOJYQ6UNY4R0XUVY35A53L47")  # Replace with your S3 access key
    .option("spark.hadoop.fs.s3a.secret.key", "UV0H7MFJI96MCFY03IMD9X6ZT8H43B3GDS8SXGA4Z7FH0QT2DSZV3ZCKTHS93Z")  # Replace with your S3 secret key
    .option("spark.hadoop.fs.s3a.endpoint", "https://54.251.141.200:9000")  # Replace with the S3 endpoint if needed (e.g., for different regions)
    .option("path",delta_path)
    .option("checkpointLocation", delta_path)  # Checkpoint location for fault tolerance
    .start()
)

# Wait for the streaming query to finish
query.awaitTermination()
