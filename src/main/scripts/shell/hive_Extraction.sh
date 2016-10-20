#!/bin/bash
export HADOOP_CLIENT_OPTS="-Xmx4g"
export HADOOP_CLASSPATH=hive-exec-0.13.0-mapr-1508-21228.jar;

/opt/mapr/hive/hive-0.13/bin/hive -hiveconf warehouseDir=$1 -hiveconf queueName=$2 -hiveconf hiveoutput_Path=$3 -hiveconf testcustomer_DB=$4 -hiveconf testcustomer_table=$5 
-f hive_Extraction.hql

 