



. $HOMEDIR/$PROPERTY_FILE &> /dev/null

# lets build the query we will execute in the hive shell
my_query="set mapred.job.queue.name=$queueName;"
my_query="$my_query INSERT INTO TABLE testemp_run_dates "
my_query="$my_query SELECT from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') "
my_query="$my_query FROM testempdb.testemp_run_dates LIMIT 1;"


