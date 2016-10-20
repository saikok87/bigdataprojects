package com.bigdata.bdp.hbaseextract;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.bdp.constants.Constants;
import com.bigdata.bdp.utils.HBaseUtility;
import com.bigdata.bdp.utils.BigDataUtils;

public class HBaseM7ExtractDriver extends Configured implements Tool, Constants  {
	
	private static final Logger logger = LoggerFactory.getLogger(HBaseM7ExtractDriver.class);
	private Configuration conf;
	private static FileSystem fs;
	private static Properties props;
	
	@Override
	public int run(String[] args) throws Exception {
		String configFile = args[0];
		String tableName = args[1];
		String columnFamily = args[2];
		String outputLocation = args[3];
		
		Path outputPath = new Path(outputLocation);
		conf = getConf();
		
		//get hold of hadoop file system
		fs = FileSystem.get(conf);
		
		props = BigDataUtils.loadProperties(configFile, fs);
		
		conf = HBaseConfiguration.create();
		
		conf.set(MAPRED_JOB_QUEUE_NAME, props.getProperty(MAPRED_JOB_QUEUE_NAME));
		conf.set(HBASE_TABLE_NAME, tableName);
		conf.set(HBASE_TABLE_COLUMN_FAMILY,columnFamily);
		conf.set(HBASE_COLUMN_NAME, props.getProperty(HBASE_COLUMN_NAME));
		conf.set(EXTRACT_OUTPUT_PATH,outputLocation);
		//conf.set("mapred.textoutputformat.separator", ",");
		
		HBaseUtility utility = new HBaseUtility();
		
		if(!utility.checkIfTableExsits(conf, tableName)){
			logger.warn("HBase table " + tableName + " does not exists!!");
			return -1;
		}
		
		Job job = Job.getInstance(conf, "HBase M7 extract");
		
		job.setJarByClass(HBaseM7ExtractDriver.class);
		
		Scan scan = new Scan();
		
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		
		TableMapReduceUtil.initTableMapperJob(tableName, scan, 
				HBaseM7ExtractMapper.class, 
				NullWritable.class, 
				Text.class, 
				job);
		
		job.setNumReduceTasks(0);
		
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		fs.delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		int status = job.waitForCompletion(true) ? 0 : 1;
		return status;
		
	}
	
	public static void main(String args[]) throws Exception{
		logger.info("Initiating Main method of HBase Driver class for extract");
		
		HBaseM7ExtractDriver driver = new HBaseM7ExtractDriver();
		driver.initiate(args);
		
	}

	private void initiate(String[] args) {
		int exit = 0;
		
		try {
			exit = ToolRunner.run(new HBaseM7ExtractDriver(), args);
			if(exit != 0){
				logger.error("HBase extraction Job failed with error exit code "+ exit);
				throw new Exception();
			}
		} catch (Exception e) {
			logger.error("HBase Extract Job failed with error exit code:" +exit,e);
		}
	}

}
