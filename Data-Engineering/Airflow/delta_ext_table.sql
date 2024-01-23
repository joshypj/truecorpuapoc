 CREATE EXTERNAL TABLE `true_corp.mysql_tpcc_orders`(
	  `col` string)
	ROW FORMAT SERDE 
	  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
	WITH SERDEPROPERTIES ( 
	  'path'='s3a://truecorp-cdc-bkt/mysql_orders') 
	STORED AS INPUTFORMAT 
	  'org.apache.hadoop.mapred.SequenceFileInputFormat' 
	OUTPUTFORMAT 
	  'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'
	LOCATION
	  's3a://truecorp-cdc-bkt/mysql_orders'
	TBLPROPERTIES (
	  'location'='s3a://truecorp-cdc-bkt/mysql_orders', 
  	  'spark.sql.sources.provider'='DELTA')
 