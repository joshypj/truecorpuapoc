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
# Connects to EzPresto over SSL, executes a SQL query, and loads the result into a Spark DataFrame
df = spark.read.format("jdbc").\
      option("driver", "com.facebook.presto.jdbc.PrestoDriver").\
      option("url", "jdbc:presto://ezpresto.truepoc.ezapac.com:443").\
      option("SSL", "true").\
      option("IgnoreSSLChecks", "true").\
      option("query", "select * from nyctaxi.default.test").\
      load()

print(df.show(5))
# Save the DataFrame as a Delta table in the S3 bucket ---Might takes some time---
df.write.format("delta").mode("overwrite").save(delta_path)