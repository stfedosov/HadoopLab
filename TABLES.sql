//------------------------
// product_purchase table:
//------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS 
product_purchase(product STRING, price DOUBLE, pdate TIMESTAMP, category STRING, ip STRING) 
PARTITIONED BY (dateval STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( "separatorChar" = "," ) LOCATION '/user/cloudera/flume/events' 

// partitions:
ALTER TABLE product_purchase ADD PARTITION (dateval = '2018-8-1') 
LOCATION '/user/cloudera/flume/events/2018/8/1';
ALTER TABLE product_purchase ADD PARTITION (dateval = '2018-8-2') 
LOCATION '/user/cloudera/flume/events/2018/8/2';
ALTER TABLE product_purchase ADD PARTITION (dateval = '2018-8-3') 
LOCATION '/user/cloudera/flume/events/2018/8/3';
ALTER TABLE product_purchase ADD PARTITION (dateval = '2018-8-4') 
LOCATION '/user/cloudera/flume/events/2018/8/4';
ALTER TABLE product_purchase ADD PARTITION (dateval = '2018-8-5') 
LOCATION '/user/cloudera/flume/events/2018/8/5';
ALTER TABLE product_purchase ADD PARTITION (dateval = '2018-8-6') 
LOCATION '/user/cloudera/flume/events/2018/8/6';
ALTER TABLE product_purchase ADD PARTITION (dateval = '2018-8-7') 
LOCATION '/user/cloudera/flume/events/2018/8/7';

//--------------
// MYSQL tables: 
//--------------
CREATE TABLE most_spending_countries (name VARCHAR(50), total DOUBLE);
CREATE TABLE most_frequent_products (name VARCHAR(50), total INTEGER);
CREATE TABLE most_frequent_categories (name VARCHAR(50), total INTEGER);
