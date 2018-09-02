# cca-175_preparation
Problems and solutions to CCA-175 exam

The tasklist based on http://arun-teaches-u-tech.blogspot.com/p/certification-preparation-plan.html website. 

__Original Description__
1. Using sqoop, import orders table into hdfs to folders /user/cloudera/problem1/orders. File should be loaded as Avro File and use snappy compression

2. Using sqoop, import order_items table into hdfs to folders /user/cloudera/problem1/order_items. Files should be loaded as avro file and use snappy compression

3. Using Spark Scala load data at /user/cloudera/problem1/orders and /user/cloudera/problem1/orders_items items as dataframes. 

4. Expected Intermediate Result: order_date , order_status, total_orders, total_amount. In plain english, please find total orders and total amount per status per day. The result should be sorted by order date in descending, order status in ascending and total amount in descending and total orders in ascending. Aggregation should be done using below methods. However, sorting can be done using a dataframe or RDD. Perform aggregation in each of the following ways

a). Just by using Data Frames API - here order_date should be YYYY-MM-DD format

b). Using Spark SQL  - here order_date should be YYYY-MM-DD format

c). By using combineByKey function on RDDS -- No need of formatting order_date or total_amount

5. Store the result as parquet file into hdfs using gzip compression under folder
/user/cloudera/problem1/result4a-gzip
/user/cloudera/problem1/result4b-gzip
/user/cloudera/problem1/result4c-gzip

6. Store the result as parquet file into hdfs using snappy compression under folder
/user/cloudera/problem1/result4a-snappy
/user/cloudera/problem1/result4b-snappy
/user/cloudera/problem1/result4c-snappy

7. Store the result as CSV file into hdfs using No compression under folder
/user/cloudera/problem1/result4a-csv
/user/cloudera/problem1/result4b-csv
/user/cloudera/problem1/result4c-csv

8. Create a mysql table named result and load data from /user/cloudera/problem1/result4a-csv to mysql table named result 

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
cloudera@quickstart:~$ spark-shell --master yarn-client --packages com.databricks:spark-avro_2.11:2.0.1
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

If we execute the ```scala df_join.show command```, then the result is:

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
val df_result2 = sqlContext.sql("select to_date(from_unixtime(order_date/1000)) as order_date, order_status , sum(order_item_subtotal) as total_amount,count(distinct order_id) total_order from item_orders_table group by order_date, order_status order by 1 desc, 2 asc, 3 asc, 4 desc")
```

```scala 
df_result2.show
+----------+---------------+------------------+-----------+                     
|order_date|   order_status|      total_amount|total_order|
+----------+---------------+------------------+-----------+
|2013-10-24|        PENDING| 5429.340110778809|          8|
|2014-03-28|       COMPLETE|25668.330516815186|         42|
|2013-09-20|        ON_HOLD| 6630.500070571899|          8|
|2014-05-01|       COMPLETE|30957.470586776733|         47|
|2014-05-31|PENDING_PAYMENT|17941.730337142944|         26|
|2014-03-29|       COMPLETE|21106.740438461304|         31|
|2013-09-20|        PENDING|  8703.11019897461|         11|
|2014-05-15|SUSPECTED_FRAUD| 2939.680088043213|          5|
|2014-06-26|         CLOSED| 11482.98017501831|         16|
|2014-02-01|     PROCESSING| 18813.75033569336|         33|
|2014-06-27|         CLOSED|12137.790252685547|         20|
|2013-12-29|     PROCESSING| 7238.240139007568|         11|
|2013-12-30|     PROCESSING|12689.590181350708|         19|
|2014-05-24|         CLOSED|11293.700244903564|         19|
|2014-04-12|SUSPECTED_FRAUD| 849.9100112915039|          3|
|2013-09-21|        ON_HOLD| 4219.470050811768|          8|
|2014-04-12|       CANCELED| 2729.640064239502|          6|
|2013-10-24|        ON_HOLD|3263.6100883483887|          6|
|2014-05-15|       CANCELED|1749.8300399780273|          4|
|2014-04-11|SUSPECTED_FRAUD|2447.7800464630127|          4|
+----------+---------------+------------------+-----------+
only showing top 20 rows

```

```scala 
```

```scala 
```



