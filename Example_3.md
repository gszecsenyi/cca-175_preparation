# cca-175_preparation
Problems and solutions to CCA-175 exam

The tasklist based on http://arun-teaches-u-tech.blogspot.com/p/certification-preparation-plan.html website. 

__Original Description__

1. Import all tables from mysql database into hdfs as avro data files. use compression and the compression codec should be snappy. data warehouse directory should be retail_stage.db

2. Create a metastore table that should point to the orders data imported by sqoop job above. Name the table orders_sqoop. 

3. Write query in hive that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from orders_sqoop. 
4. Query table in impala that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from order_sqoop. 

4. Now create a table named retail.orders_avro in hive stored as avro, the table should have same table definition as order_sqoop. Additionally, this new table should be partitioned by the order month i.e -> year-order_month.(example: 2014-01)
Load data into orders_avro table from orders_sqoop table.

5. Write query in hive that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from orders_avro
evolve the avro schema related to orders_sqoop table by adding more fields named (order_style String, order_zone Integer)
insert two more records into orders_sqoop table. 

6. Write query in hive that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from orders_sqoop
query table in impala that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from orders_sqoop

__My solution with Spark 1.6 and Scala__

1.

```console
[cloudera@quickstart ~]$ sqoop import-all-tables --connect jdbc:mysql://quickstart:3306/retail_db --username=retail_dba --password=cloudera --compression-codec=snappy --as-avrodatafile --warehouse-dir=/user/cloudera/problem3/retail_Stage.db -m 1
```

2.
```console
[cloudera@quickstart ~]$ hadoop fs -get /user/cloudera/problem3/retail_Stage.db/orders/part-m-00000.avro
[cloudera@quickstart ~]$ avro-tools getschema part-m-00000.avro > orders_sqoop.avsc
```

Open hive, and execute the following statement:
```hive
CREATE EXTERNAL TABLE orders_sqoop(
  order_id int,
  order_date bigint,
  order_customer_id int ,
  order_status string )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  '/user/cloudera/problem3/retail_Stage.db/orders'
TBLPROPERTIES (
  'avro.schema.url'='/user/cloudera/problem3/orders_sqoop.avsc');
```

3.

```console
select * from orders_sqoop y where y.order_date in (select z.order_date from (select x.order_date, count(1) from orders_sqoop x group by x.order_date order by 2 desc limit 1) z);
```
4.
Exit from hive with exit command, and open impala with impala command. Then execute the following commands:
```console
invalidate metadata;
select * from orders_sqoop y where y.order_date in (select z.order_date from (select x.order_date, count(1) from orders_sqoop x group by x.order_date order by 2 desc limit 1) z);
```
Now open again hive with hive command and execute the following commands:
```hive
create table orders_avro (
order_id int,
order_date bigint,
order_customer_id int,
order_status string)
PARTITIONED BY (order_yearmonth string)
STORED AS AVRO
;
```

```scala

```

```scala

```

```scala

```

```scala

```

```hive

```
