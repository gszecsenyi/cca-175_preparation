# cca-175_preparation
Problems and solutions to CCA-175 exam

The tasklist based on http://arun-teaches-u-tech.blogspot.com/p/certification-preparation-plan.html website. 

__Original Description__

1.  Import orders table from mysql as text file to the destination /user/cloudera/problem5/text. Fields should be terminated by a tab character ("\t") character and lines should be terminated by new line character ("\n"). 
2.  Import orders table from mysql  into hdfs to the destination /user/cloudera/problem5/avro. File should be stored as avro file.
3.  Import orders table from mysql  into hdfs  to folders /user/cloudera/problem5/parquet. File should be stored as parquet file.
4.  Transform/Convert data-files at /user/cloudera/problem5/avro and store the converted file at the following locations and file formats
    -   save the data to hdfs using snappy compression as parquet file at /user/cloudera/problem5/parquet-snappy-compress
    -   save the data to hdfs using gzip compression as text file at /user/cloudera/problem5/text-gzip-compress
    -   save the data to hdfs using no compression as sequence file at /user/cloudera/problem5/sequence
    -   save the data to hdfs using snappy compression as text file at /user/cloudera/problem5/text-snappy-compress
5.  Transform/Convert data-files at /user/cloudera/problem5/parquet-snappy-compress and store the converted file at the following locations and file formats
save the data to hdfs using no compression as parquet file at /user/cloudera/problem5/parquet-no-compress
save the data to hdfs using snappy compression as avro file at /user/cloudera/problem5/avro-snappy
6.  Transform/Convert data-files at /user/cloudera/problem5/avro-snappy and store the converted file at the following locations and file formats
save the data to hdfs using no compression as json file at /user/cloudera/problem5/json-no-compress
save the data to hdfs using gzip compression as json file at /user/cloudera/problem5/json-gzip
7.  Transform/Convert data-files at  /user/cloudera/problem5/json-gzip and store the converted file at the following locations and file formats
save the data to as comma separated text using gzip compression at   /user/cloudera/problem5/csv-gzip
8.  Using spark access data at /user/cloudera/problem5/sequence and stored it back to hdfs using no compression as ORC file to HDFS to destination /user/cloudera/problem5/orc 


__My solution with Spark 1.6 and Scala__

1.

```console
[cloudera@quickstart ~]$ sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table orders --warehouse-dir=/user/cloudera/problem5/text --as-textfile --fields-terminated-by='\t'
```

2.
```console
[cloudera@quickstart ~]$ sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table orders --warehouse-dir=/user/cloudera/problem5/avro --as-avrodatafile 
```

3.
```console
[cloudera@quickstart ~]$ sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table orders --warehouse-dir=/user/cloudera/problem5/parquet --as-parquetfile 
```

4.
Open Spark-shell and execute the following command
```scala
import com.databricks.spark.avro._ 

val avro_df = sqlContext.read.avro("/user/cloudera/problem5/avro/orders")

```
