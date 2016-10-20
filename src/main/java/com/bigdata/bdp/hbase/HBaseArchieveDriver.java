package com.bigdata.bdp.hbase;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdataqueue.bdp.constants.YodleeConstants;
import com.bigdataqueue.bdp.utils.YodleeUtils;

/**
 * This is driver class for archiving the records from Hbase M7 table
 * 
 * @author Sai Kokadwar
 *
 */
public class HBaseArchieveDriver extends Configured implements Tool, YodleeConstants {
	
	private static final Logger logger = LoggerFactory.getLogger(HBaseArchieveDriver.class);
	
	private static Properties props;
	private static FileSystem fs;
	
	@Override
	public int run(String[] args) throws Exception {
		
		int status = 0;
		
		String outputDataset = args[0];
		String configFile = args[1];
		String dailyTable = args[2];
		String daily_col_family = args[3];
		
		Configuration conf = getConf();
		//get hold of hadoop file system
		fs = FileSystem.get(conf);
		//load properties file
		props = YodleeUtils.loadProperties(configFile, fs);
		
		conf = HBaseConfiguration.create();
		
		conf.set(MAPRED_JOB_QUEUE_NAME, props.getProperty(MAPRED_JOB_QUEUE_NAME));
		conf.set(HBASE_TABLE_NAME, dailyTable);
		conf.set(HBASE_TABLE_COLUMN_FAMILY,daily_col_family);
		conf.set(EXTRACT_OUTPUT_PATH,outputDataset);
		conf.set(ARCHIEVETIME,props.getProperty(ARCHIEVETIME));
		
		//delete outputpath if already exists
		if(fs.exists(new Path(outputDataset))) {
			FileSystem.get(conf).delete(new Path(outputDataset), true);
			logger.debug("Output path deleted successfully");
		}
		
		Job job = Job.getInstance(conf, "HBase table archiever");
		job.setJarByClass(HBaseArchieveDriver.class);
		job.setNumReduceTasks(0);
		
		FileOutputFormat.setOutputPath(job, new Path(outputDataset)); //set output path
		
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		
		TableMapReduceUtil.
		
		
		
		
		
		return status;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
}
