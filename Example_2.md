# cca-175_preparation
Problems and solutions to CCA-175 exam

The tasklist based on http://arun-teaches-u-tech.blogspot.com/p/certification-preparation-plan.html website. 

__Original Description__
1. Using sqoop copy data available in mysql products table to folder /user/cloudera/products on hdfs as text file. columns should be delimited by pipe '|'

2. Move all the files from /user/cloudera/products folder to /user/cloudera/problem2/products folder

3. Change permissions of all the files under /user/cloudera/problem2/products such that owner has read,write and execute permissions, group has read and write permissions whereas others have just read and execute permissions
4. Read data in /user/cloudera/problem2/products and do the following operations using 

a) dataframes api 
b) spark sql 
c) RDDs aggregateByKey method. 

Your solution should have three sets of steps. Sort the resultant dataset by category id

Filter such that your RDD\DF has products whose price is lesser than 100 USD
- on the filtered data set find out the higest value in the product_price column under each category
- on the filtered data set also find out total products under each category
- on the filtered data set also find out the average price of the product under each category
- on the filtered data set also find out the minimum price of the product under each category

5. Store the result in avro file using snappy compression under these folders respectively
/user/cloudera/problem2/products/result-df
/user/cloudera/problem2/products/result-sql
/user/cloudera/problem2/products/result-rdd

__My solution with Spark 1.6 and Scala__

1.

```console
cloudera@quickstart:~$ sqoop import --table orders --connect jdbc:mysql://quickstart:3306/retail_db \
--username=retail_dba --password=cloudera --compression-codec=snappy --as-avrodatafile \
--warehouse-dir=/user/cloudera/problem1
```

2.

```console
cloudera@quickstart:~$ sqoop import --table order_items --connect jdbc:mysql://quickstart:3306/retail_db \
--username=retail_dba --password=cloudera --compression-codec=snappy --as-avrodatafile \
--warehouse-dir=/user/cloudera/problem1
```

3.

```console
cloudera@quickstart:~$ export JAVA_TOOL_OPTIONS="-Dhttps.protocols=TLSv1.2"
cloudera@quickstart:~$ spark-shell --master yarn-client --packages com.databricks:spark-csv_2.11:1.5.0,com.databricks:spark-avro_2.11:2.0.1

```

```scala
import com.databricks.spark.avro._

val df_orders = sqlContext.read.avro("/user/cloudera/problem1/orders")
val df_order_items = sqlContext.read.avro("/user/cloudera/problem1/order_items")

```

4. 

a).
```scala
val df_join = df_orders.join(df_order_items,$"order_id" === $"order_item_order_id","inner")
val df_result = df_join.select(to_date(from_unixtime($"order_date"/1000)).alias("order_date"),$"order_status", $"order_item_subtotal".alias("total_amount"),$"order_id").groupBy($"order_date",$"order_status").agg(sum($"total_amount").alias("total_amount"), countDistinct($"order_id").alias("total_order")).orderBy($"order_date".desc, $"order_status".asc, $"total_amount".desc,$"total_order".asc)

```

If we execute the ```scala 
df_join.show``` command, then the result is:

```scala 
+----------+---------------+------------------+-----------+                     
|order_date|   order_status|      total_amount|total_order|
+----------+---------------+------------------+-----------+
|2014-07-24|       CANCELED|1254.9200382232666|          2|
|2014-07-24|         CLOSED|16333.160339355469|         26|
|2014-07-24|       COMPLETE| 34552.03063583374|         55|
|2014-07-24|        ON_HOLD|1709.7400207519531|          4|
|2014-07-24| PAYMENT_REVIEW|499.95001220703125|          1|
|2014-07-24|        PENDING|12729.490217208862|         22|
|2014-07-24|PENDING_PAYMENT|17680.700359344482|         34|
|2014-07-24|     PROCESSING| 9964.740190505981|         17|
|2014-07-24|SUSPECTED_FRAUD|2351.6100215911865|          4|
|2014-07-23|       CANCELED| 5777.330112457275|         10|
|2014-07-23|         CLOSED|  13312.7202835083|         18|
|2014-07-23|       COMPLETE|25482.510496139526|         40|
|2014-07-23|        ON_HOLD| 4514.460060119629|          6|
|2014-07-23| PAYMENT_REVIEW|1699.8200302124023|          2|
|2014-07-23|        PENDING|   6161.3701171875|         11|
|2014-07-23|PENDING_PAYMENT|19279.810424804688|         30|
|2014-07-23|     PROCESSING| 7962.790130615234|         15|
|2014-07-23|SUSPECTED_FRAUD|3799.5700721740723|          6|
|2014-07-22|       CANCELED| 3209.730094909668|          4|
|2014-07-22|         CLOSED| 12688.79024887085|         20|
+----------+---------------+------------------+-----------+
only showing top 20 rows

```

b.)
Register temp table into Spark SQL Context
```scala 
df_join.registerTempTable("item_orders_table")
```
Execute query
```scala 
val df_result2 = sqlContext.sql("select to_date(from_unixtime(order_date/1000)) as order_date, order_status , sum(order_item_subtotal) as total_amount,count(distinct order_id) total_order from item_orders_table group by order_date, order_status order by order_date desc, order_status asc, total_amount asc, total_order desc")
```

```scala 
df_result2.show
+----------+---------------+------------------+-----------+                     
|order_date|   order_status|      total_amount|total_order|
+----------+---------------+------------------+-----------+
|2014-07-24|       CANCELED|1254.9200382232666|          2|
|2014-07-24|         CLOSED|16333.160339355469|         26|
|2014-07-24|       COMPLETE| 34552.03063583374|         55|
|2014-07-24|        ON_HOLD|1709.7400207519531|          4|
|2014-07-24| PAYMENT_REVIEW|499.95001220703125|          1|
|2014-07-24|        PENDING|12729.490217208862|         22|
|2014-07-24|PENDING_PAYMENT|17680.700359344482|         34|
|2014-07-24|     PROCESSING| 9964.740190505981|         17|
|2014-07-24|SUSPECTED_FRAUD|2351.6100215911865|          4|
|2014-07-23|       CANCELED| 5777.330112457275|         10|
|2014-07-23|         CLOSED|  13312.7202835083|         18|
|2014-07-23|       COMPLETE|25482.510496139526|         40|
|2014-07-23|        ON_HOLD| 4514.460060119629|          6|
|2014-07-23| PAYMENT_REVIEW|1699.8200302124023|          2|
|2014-07-23|        PENDING|   6161.3701171875|         11|
|2014-07-23|PENDING_PAYMENT|19279.810424804688|         30|
|2014-07-23|     PROCESSING| 7962.790130615234|         15|
|2014-07-23|SUSPECTED_FRAUD|3799.5700721740723|          6|
|2014-07-22|       CANCELED| 3209.730094909668|          4|
|2014-07-22|         CLOSED| 12688.79024887085|         20|
+----------+---------------+------------------+-----------+
only showing top 20 rows
```
c.)

5.

a.)
```scala 
sqlContext.setConf("spark.sql.parquet.compression.codec","gzip")
df_result.write.parquet("/user/cloudera/problem1/result4a-gzip")
``` 

b.)
```scala 
sqlContext.setConf("spark.sql.parquet.compression.codec","gzip")
df_result2.write.parquet("/user/cloudera/problem1/result4b-gzip")
``` 
c.)
```scala

```
6.
a.)
```scala
sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
df_result.write.parquet("/user/cloudera/problem1/result4a-snappy")
```

b.)
```scala
df_result2.write.parquet("/user/cloudera/problem1/result4b-snappy")
```

c.)
```scala

```

7.
a.)
```scala
df_result.rdd.saveAsTextFile("/user/cloudera/problem1/result4a-csv")
```

b.)
```scala
df_result2.rdd.saveAsTextFile("/user/cloudera/problem1/result4b-csv")
```

c.)
```scala

```

mysql -u retail_dba -p
use retail_db;
create table result(order_date varchar(10),order_status varchar(40), order_amount float, order_count int);

