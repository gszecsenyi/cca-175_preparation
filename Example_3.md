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

5. Add to columns to the orders_sqoop table, with the following attributes (order_zone int, order_style string)

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

```hive
select * from orders_sqoop y where y.order_date in (select z.order_date from (select x.order_date, count(1) from orders_sqoop x group by x.order_date order by 2 desc limit 1) z);
```
4.
Exit from hive with exit command, and open impala with impala command. Then execute the following commands:
```impala
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
partitioned by (order_month string)
stored as avro;
```

```hive
insert overwrite table orders_avro partition (order_month)
select order_id, order_date, order_customer_id, order_status, substr(from_unixtime(cast(order_date/1000 as int)),1,7) as order_month from orders_sqoop;
```
5.
Add the following entry to the orders_sqoop.avsc and upload (overwrite) it:
```hive
, {
    "name" : "order_zone",
    "type" : [ "null", "int" ],
    "default" : null,
    "columnName" : "order_zone",
    "sqlType" : "4"
  }, {
    "name" : "order_style",
    "type" : [ "null", "string" ],
    "default" : null,
    "columnName" : "order_style",
    "sqlType" : "12"
  }
```

so the full file content is:
```hive
{
  "type" : "record",
  "name" : "orders",
  "doc" : "Sqoop import of orders",
  "fields" : [ {
    "name" : "order_id",
    "type" : [ "null", "int" ],
    "default" : null,
    "columnName" : "order_id",
    "sqlType" : "4"
  }, {
    "name" : "order_date",
    "type" : [ "null", "long" ],
    "default" : null,
    "columnName" : "order_date",
    "sqlType" : "93"
  }, {
    "name" : "order_customer_id",
    "type" : [ "null", "int" ],
    "default" : null,
    "columnName" : "order_customer_id",
    "sqlType" : "4"
  }, {
    "name" : "order_status",
    "type" : [ "null", "string" ],
    "default" : null,
    "columnName" : "order_status",
    "sqlType" : "12"
  }, {
    "name" : "order_zone",
    "type" : [ "null", "int" ],
    "default" : null,
    "columnName" : "order_zone",
    "sqlType" : "4"
  }, {
    "name" : "order_style",
    "type" : [ "null", "string" ],
    "default" : null,
    "columnName" : "order_style",
    "sqlType" : "12"
  } ],
  "tableName" : "orders"
}
```

Open hive, and check for the table description:
```hive
desc orders_sqoop;
```