#!/bin/sh
#Usage  : ./hive_xml_copy.sh $1 $2

echo SOURCE_DIR=$1
echo TARGET_DIR=$2

echo hadoop fs -put -f $1/hive-site.xml $2
hadoop fs -put -f $1/hive-site.xml $2

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
echo "HIVE SITE XML File Copy Successful"
else
echo "HIVE SITE XML File Copy Failed with Exit Code $EXIT_CODE"
exit $EXIT_CODE
fi