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
a.) /user/cloudera/problem2/products/result-df
b.) /user/cloudera/problem2/products/result-sql
c.) /user/cloudera/problem2/products/result-rdd

__My solution with Spark 1.6 and Scala__

1.

```console
cloudera@quickstart:~$ sqoop import --table=products --connect jdbc:mysql://quickstart:3306/retail_db --username=retail_dba --password=cloudera --fields-terminated-by \| --lines-terminated-by \\n --warehouse-dir=/user/cloudera
```

2.

```console
cloudera@quickstart ~$ hadoop fs -mkdir /user/cloudera/problem2
cloudera@quickstart ~$ hadoop fs -mv /user/cloudera/products /user/cloudera/problem2
```

3.

```console
[cloudera@quickstart ~]$ hadoop fs -chmod 765 /user/cloudera/problem2/products/*
[cloudera@quickstart ~]$ hadoop fs -ls /user/cloudera/problem2/products
Found 5 items
-rwxrw-r-x   1 cloudera cloudera          0 2018-09-08 04:28 /user/cloudera/problem2/products/_SUCCESS
-rwxrw-r-x   1 cloudera cloudera      41419 2018-09-08 04:28 /user/cloudera/problem2/products/part-m-00000
-rwxrw-r-x   1 cloudera cloudera      43660 2018-09-08 04:28 /user/cloudera/problem2/products/part-m-00001
-rwxrw-r-x   1 cloudera cloudera      42195 2018-09-08 04:28 /user/cloudera/problem2/products/part-m-00002
-rwxrw-r-x   1 cloudera cloudera      46719 2018-09-08 04:28 /user/cloudera/problem2/products/part-m-00003

```
4.

```console
cloudera@quickstart:~$ export JAVA_TOOL_OPTIONS="-Dhttps.protocols=TLSv1.2"
cloudera@quickstart:~$ spark-shell --master yarn-client --packages com.databricks:spark-csv_2.11:1.5.0,com.databricks:spark-avro_2.11:2.0.1

```
```scala

case class Product(id: Int, category_id: Int, name: String, description:String, price: Double, image:String )
val input = sc.textFile("/user/cloudera/problem2/products/")
val cols = input.map(x => x.split('|'))
val products_df = cols.map(p => Product(p(0).toInt,p(1).toInt,p(2),p(3),p(4).toDouble,p(5))).toDF

products_df.show

+---+-----------+--------------------+-----------+------+--------------------+
| id|category_id|                name|description| price|               image|
+---+-----------+--------------------+-----------+------+--------------------+
|  1|          2|Quest Q64 10 FT. ...|           | 59.98|http://images.acm...|
|  2|          2|Under Armour Men...|           |129.99|http://images.acm...|
|  3|          2|Under Armour Men...|           | 89.99|http://images.acm...|
|  4|          2|Under Armour Men...|           | 89.99|http://images.acm...|
|  5|          2|Riddell Youth Rev...|           |199.99|http://images.acm...|
|  6|          2|Jordan Mens VI R...|           |134.99|http://images.acm...|
|  7|          2|Schutt Youth Recr...|           | 99.99|http://images.acm...|
|  8|          2|Nike Mens Vapor ...|           |129.99|http://images.acm...|
|  9|          2|Nike Adult Vapor ...|           |  50.0|http://images.acm...|
| 10|          2|Under Armour Men...|           |129.99|http://images.acm...|
| 11|          2|Fitness Gear 300 ...|           |209.99|http://images.acm...|
| 12|          2|Under Armour Men...|           |139.99|http://images.acm...|
| 13|          2|Under Armour Men...|           | 89.99|http://images.acm...|
| 14|          2|Quik Shade Summit...|           |199.99|http://images.acm...|
| 15|          2|Under Armour Kids...|           | 59.99|http://images.acm...|
| 16|          2|Riddell Youth 360...|           |299.99|http://images.acm...|
| 17|          2|Under Armour Men...|           |129.99|http://images.acm...|
| 18|          2|Reebok Mens Full...|           | 29.97|http://images.acm...|
| 19|          2|Nike Mens Finger...|           |124.99|http://images.acm...|
| 20|          2|Under Armour Men...|           |129.99|http://images.acm...|
+---+-----------+--------------------+-----------+------+--------------------+
only showing top 20 rows

```
a.)

```scala
val dfApi = products_df.filter($"price" < 100).groupBy($"category_id").agg(max($"price").alias("max_price"),count($"id").alias("total_product"),avg($"price").alias("avg_price"),min($"price").alias("min_price"))


dfApi.show
+-----------+---------+-------------+------------------+---------+
|category_id|max_price|total_product|         avg_price|min_price|
+-----------+---------+-------------+------------------+---------+
|         31|    99.99|            7| 88.56142857142856|    79.99|
|         32|    99.99|           10|             48.99|    19.99|
|         33|    99.99|           19| 58.46157894736842|     10.8|
|         34|    99.99|            9| 83.87888888888888|    34.99|
|         35|    79.99|            9| 34.21222222222222|     9.99|
|         36|    24.99|           24|19.198333333333338|    12.99|
|         37|    51.99|           24| 36.40666666666667|     4.99|
|         38|    99.95|           14|46.339285714285715|    19.99|
|         39|    34.99|           12|23.740000000000006|    19.99|
|         40|    24.99|           24|24.990000000000006|    24.99|
|         41|    99.99|           37| 31.23648648648649|     9.59|
|         42|      0.0|            1|               0.0|      0.0|
|         43|     99.0|            1|              99.0|     99.0|
|         44|    99.98|           15| 62.18933333333334|    21.99|
|         45|    99.99|            7| 55.41857142857143|    27.99|
|         46|    49.98|            9| 34.65111111111111|    19.98|
|         47|    99.95|           14| 44.63071428571429|    21.99|
|         48|    49.98|            7| 35.69714285714286|    19.98|
|         49|    99.99|           13| 74.21692307692308|    19.98|
|         50|     60.0|           14|53.714285714285715|     34.0|
+-----------+---------+-------------+------------------+---------+
only showing top 20 rows
```

b.)

```scala
products_df.registerTempTable("products")
val dfSQL = sqlContext.sql("select category_id, max(price) max_price, count(*) total_product, avg(price) avg_price, min(price) min_price from products where price < 100 group by category_id")

dfSQL.show
+-----------+---------+-------------+------------------+---------+
|category_id|max_price|total_product|         avg_price|min_price|
+-----------+---------+-------------+------------------+---------+
|         31|    99.99|            7| 88.56142857142856|    79.99|
|         32|    99.99|           10|             48.99|    19.99|
|         33|    99.99|           19| 58.46157894736842|     10.8|
|         34|    99.99|            9| 83.87888888888888|    34.99|
|         35|    79.99|            9| 34.21222222222222|     9.99|
|         36|    24.99|           24|19.198333333333338|    12.99|
|         37|    51.99|           24| 36.40666666666667|     4.99|
|         38|    99.95|           14|46.339285714285715|    19.99|
|         39|    34.99|           12|23.740000000000006|    19.99|
|         40|    24.99|           24|24.990000000000006|    24.99|
|         41|    99.99|           37| 31.23648648648649|     9.59|
|         42|      0.0|            1|               0.0|      0.0|
|         43|     99.0|            1|              99.0|     99.0|
|         44|    99.98|           15| 62.18933333333334|    21.99|
|         45|    99.99|            7| 55.41857142857143|    27.99|
|         46|    49.98|            9| 34.65111111111111|    19.98|
|         47|    99.95|           14| 44.63071428571429|    21.99|
|         48|    49.98|            7| 35.69714285714286|    19.98|
|         49|    99.99|           13| 74.21692307692308|    19.98|
|         50|     60.0|           14|53.714285714285715|     34.0|
+-----------+---------+-------------+------------------+---------+
only showing top 20 rows


```
c.)

```scala
val dfRDD = products_df.filter($"price" < 100).map(x => (x(1).toString.toInt,x(4).toString.toDouble)).aggregateByKey((0.0,99999999999999.0,0,0.0))((v1,v2) => (math.max(v1._1,v2),math.min(v1._2,v2),v1._3+1,v1._4+v2),(p1,p2) => (math.max(p1._1,p2._1),math.min(p1._2,p2._2),(p1._3 +p2._3),p1._4+p2._4)).map(x => (x._1,x._2._1,x._2._2,x._2._3,x._2._4/x._2._3)).sortBy(_._1,true).toDF
```
5. 

a.)

```scala
import com.databricks.spark.avro._;

sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
dfApi.write.avro("/user/cloudera/problem2/products/result-df")
```

b.)
```scala
import com.databricks.spark.avro._;

sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
dfSQL.write.avro("/user/cloudera/problem2/products/result-sql")
```

c.)
```scala
import com.databricks.spark.avro._;

sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
dfRDD.write.avro("/user/cloudera/problem2/products/result-rdd")
```
