# Import necessary libraries
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("ETL").getOrCreate()
from py4j.java_gateway import java_import
java_import(spark._sc._jvm, "org.apache.spark.sql.api.python.*")
from delta.tables import DeltaTable
print('""""""""""""""""""""""""""""""""""""""""""""""""""""""""""')
print('""""""""""""""""Executing the python script"""""""""""""""')


# Define the folder path
delta_path = "s3a://truepoc-bkt-raw/test1234/"
# Read Data from Delta
df = spark.read.format("delta").load(delta_path)
df.show(5)