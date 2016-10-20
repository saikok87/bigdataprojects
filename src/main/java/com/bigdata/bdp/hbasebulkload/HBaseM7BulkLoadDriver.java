package com.bigdata.bdp.hbasebulkload;



import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.bigdata.bdp.constants.Constants;
import com.bigdata.bdp.utils.HBaseUtility;
import com.bigdata.bdp.utils.BigDataUtils;

public class HBaseM7BulkLoadDriver extends Configured implements Tool, Constants {

	private static Logger logger = Logger.getLogger(HBaseM7BulkLoadDriver.class);
	private Configuration conf;
	private static Properties props;
	private static FileSystem fs;
	
	@Override
	public int run(String[] args) throws Exception {
		String configFile = args[0];
		String inputPath = args[1];
		String hbaseTable = args[2];
		String columnFamily = args[3];
		String hbaseClientJar = args[4];
		String hbaseServerJar = args[5];
		String hbaseCommonJar = args[6];
		
		HBaseUtility utility = new HBaseUtility();
		
		conf = getConf();
		//conf.addResource(new Path("/opt/mapr/hbase/hbase-0.98.12/conf/hbase-site.xml"));
		//conf.addResource(new Path("/opt/mapr/hadoop/hadoop-2.5.1/etc/hadoop/yarn-site.xml"));
		
		
		fs = FileSystem.get(conf);
		
		props = BigDataUtils.loadProperties(configFile,fs);
		
		conf = HBaseConfiguration.create();
		
		Path path = new Path(inputPath);
		
		if(!fs.exists(path)){
			logger.warn("Input directory " + inputPath + " does not exist.");
			return -1;
		}
		
		conf.set(MAPRED_JOB_QUEUE_NAME, props.getProperty(MAPRED_JOB_QUEUE_NAME));
		//conf.set("hbase.zookeeper.quorum", props.getProperty(HBASE_ZOOKEEPER_SERVER));
		//conf.set("hbase.zookeeper.property.clientPort", props.getProperty(HBASE_ZOOKEEPER_CLIENT_PORT));
		conf.set(HBASE_TABLE_COLUMN_FAMILY,columnFamily);
		conf.set(HBASE_COLUMN_NAME, props.getProperty(HBASE_COLUMN_NAME));
		conf.set(HBASE_ROWKEY_INDEX, props.getProperty(HBASE_ROWKEY_INDEX));
		
		Job job = Job.getInstance(conf, "MapReduce: Bulk Insertion code for HBase");
		
		job.setJarByClass(HBaseM7BulkLoadDriver.class);
		job.setMapperClass(HBaseM7BulkLoadMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		
		job.addFileToClassPath(new Path(hbaseClientJar));
		job.addFileToClassPath(new Path(hbaseServerJar));
		job.addFileToClassPath(new Path(hbaseCommonJar));
		
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, path);
		job.setOutputFormatClass(TableOutputFormat.class);
		
		logger.info("checking table exists: " + utility.checkIfTableExsits(conf, hbaseTable));
		
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, hbaseTable);
		
		int status = job.waitForCompletion(true) ? 0 : 1;
		
		return status;
	}

	public static void main(String[] args){
		logger.info("Initiating Main method of HBase Driver class for Bulk Load");
		HBaseM7BulkLoadDriver driver = new HBaseM7BulkLoadDriver();
		driver.init(args);
	}

	private void init(String[] args) {
		int exit = 0;
		
		try {
			exit = ToolRunner.run(new HBaseM7BulkLoadDriver(), args);
			
			if(exit != 0){
				logger.error("HBase Bulk Load Job failed with error exit code "+ exit);
			}
		} catch (Exception e) {
			logger.error("HBase Bulk Load Job failed with error exit code " + exit,e);
		}
		
	}
	

}
