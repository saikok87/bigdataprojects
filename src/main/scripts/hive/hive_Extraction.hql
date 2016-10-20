SET mapreduce.job.queuename=${queueName};

SET hive.metastore.warehouse.dir=${warehouseDir};

CREATE DATABASE IF NOT EXISTS ${testcustomer_DB};

USE ${testcustomer_DB};

CREATE TABLE IF NOT EXISTS ${testcustomer_table}(
name STRING COMMENT 'name of the customer',
state STRING COMMENT 'state of the customer'
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ',';

INSERT OVERWRITE DIRECTORY '${hiveoutput_Path}'
SELECT name,state
FROM ${testcustomer_DB}.${testcustomer_table}
;

