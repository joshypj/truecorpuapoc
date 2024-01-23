print('""""""""""""""""""""""""""""""""""""""""""""""""""""""""""')
print('""""""""""""""""Executing the python kafka topic read script"""""""""""""""')
# Import necessary libraries
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("ETL").getOrCreate()
spark.sparkContext.setLogLevel("ALL")
from py4j.java_gateway import java_import
java_import(spark._sc._jvm, "org.apache.spark.sql.api.python.*")
from pyspark.sql.functions import from_json
from pyspark.sql.types import StringType, StructType
from delta.tables import DeltaTable


# Define the folder path
delta_path = "s3a://truepoc-bkt-raw/test1234/"
# Connects to EzPresto over SSL, executes a SQL query, and loads the result into a Spark DataFrame
bootstrap_servers = ['10.107.94.122:32670','10.21.0.223:32305','10.21.0.208:31908']
topic = 'First_Topic'


df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", '10.97.23.65:9092')
    .option("subscribe", topic)
    .option("kafka.security.protocol", "PLAINTEXT")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.username", 'user1')
    .option("kafka.sasl.password", 'user1') 
    .option("includeHeaders", "true") 
    .load()
)

query = (
    df
    .writeStream
    .format("parquet")  # You can choose the desired format (e.g., parquet, csv, etc.)
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
    .option("path","s3a://truepoc-bkt-raw/kafka/")
    .option("checkpointLocation", "s3a://truepoc-bkt-raw/kafka/")  # Checkpoint location for fault tolerance
    .start()
)

# Wait for the streaming query to finish
query.awaitTermination()
