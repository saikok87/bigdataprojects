#!/bin/bash
HOMEDIR=$1
echo "HOMEDIR : $HOMEDIR"
PROPERTY_FILE=$2
testemp_DB=$3
queueName=$4
testemp_table=$5
hiveinput_Path=$6
cursor="'"
comma=","
star="*"

. $HOMEDIR/$PROPERTY_FILE &> /dev/null

# lets build the query we will execute in the hive shell
my_query="set mapred.job.queue.name=$queueName;"
my_query="$my_query CREATE DATABASE IF NOT EXISTS $testemp_DB;"
my_query="$my_query CREATE EXTERNAL TABLE $testemp_DB.$testemp_table( "
my_query="$my_query empid string, "
my_query="$my_query name string) "
my_query="$my_query ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' "
my_query="$my_query LINES TERMINATED BY '\n' "
my_query="$my_query STORED AS TEXTFILE "
my_query="$my_query LOCATION '$hiveinput_Path';"


# echo the query passed to the hive shell just because
echo "hive -S -e \"$my_query\""
my_value=$(hive -e "$my_query")
echo "my_value=$my_value"
