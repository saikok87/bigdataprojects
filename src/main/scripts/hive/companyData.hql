-- SET THE DEFAULT PARAMETERS----

SET hive.auto.convert.join=true
SET hive.map.aggr.hash.percentmemory = 0.125;
SET hive.auto.convert.join.noconditionaltask = true;
SET hive.auto.convert.join.noconditionaltask.size = 10000000;
SET hive.exec.compress.output=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;
SET mapred.tasktracker.map.tasks.maximum=600;
SET mapred.tasktracker.reduce.tasks.maximum=600;
SET mapred.reduce.tasks=600;
SET mapred.map.tasks=600;

SET hive.metastore.warehouse.dir=${warehouseDir};

CREATE DATABASE IF NOT EXISTS ${testcustomer_DB};
USE ${testcustomer_DB};

CREATE TABLE IF NOT EXISTS ${testcustomer_table} 
(name string,
state string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA INPATH '${inputPath} INTO TABLE ${testcustomer_table};

CREATE DATABASE IF NOT EXISTS ${companyDBName};
USE ${companyDBName};

CREATE EXTERNAL TABLE IF NOT EXISTS ${companyTableName} 
(name string,
state string,
company string,
salary int) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LOCATION '${hiveExternalPathCmpny}';

INSERT OVERWRITE TABLE ${companyTableName} 
 SELECT name, state FROM ${testcustomer_DB}.${testcustomer_table};




