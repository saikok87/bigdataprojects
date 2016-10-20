#!/bin/bash
HOMEDIR=$1
echo "HOMEDIR : $HOMEDIR"
PROPERTY_FILE=$2
testcustomer_DB=$3
queueName=$4
testcustomer_table=$5
cursor="'"
comma=","
star="*"
hiveoutput_Path=$6

. $HOMEDIR/$PROPERTY_FILE &> /dev/null

# lets build the query we will execute in the hive shell
my_query="set mapred.job.queue.name=$queueName;"
my_query="$my_query INSERT OVERWRITE DIRECTORY $cursor$hiveoutput_Path$cursor"
my_query="$my_query ROW FORMAT DELIMITED"
my_query="$my_query FIELDS TERMINATED BY $cursor$comma$cursor"
my_query="$my_query SELECT $star"
my_query="$my_query FROM $testcustomer_DB.$testcustomer_table"

# echo the query passed to the hive shell just because
echo "hive -S -e \"$my_query\""
my_value=$(hive -e "$my_query")
echo "my_value=$my_value"

