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
```scala
val df_join = df_orders.join(df_order_items,$"order_id" === $"order_item_order_id","inner")
val df_result = df_join.select(to_date(from_unixtime($"order_date"/1000)).alias("order_date"),$"order_status", $"order_item_subtotal".alias("total_amount"),$"order_id").groupBy($"order_date",$"order_status").agg(sum($"total_amount").alias("total_amount"), countDistinct($"order_id").alias("total_order")).orderBy($"order_date".desc, $"order_status".asc, $"total_amount".desc,$"total_order".asc)

```



